# Consumer ClientInfo 按 Topic 索引方案

## 背景

在 RocketMQ Proxy 集群模式下，需要获取指定 `consumerGroup + topic` 下的所有 `ClientChannelInfo`。

### 框架现有数据结构的局限

`ConsumerGroupInfo` 内部有两张独立的 Map：

```
ConsumerGroupInfo (group 级别)
  ├─ channelInfoTable:   Map<Channel, ClientChannelInfo>   // 连接维度，无 topic 字段
  └─ subscriptionTable:  Map<Topic,   SubscriptionData>    // 订阅维度，无 clientId 字段
```

两张表之间**没有任何关联字段**，无法从 topic 直接导航到 ClientChannelInfo。

`ConsumerManager` 虽然维护了反向索引 `topicGroupTable: Map<topic, Set<group>>`，但只能从 topic 找到 group，再从 group 拿到**该 group 下全部 client**，无法进一步按 topic 过滤。

### 为什么不能在同一 group 内按 topic 过滤

`ConsumerGroupInfo.updateSubscription()` 的清理逻辑会把"不在本次心跳 subList 里的 topic"从 `subscriptionTable` 中删除。若同 group 不同 client 订阅不同 topic，心跳会导致订阅状态持续抖动覆盖。

RocketMQ 强制约定：**同一个 ConsumerGroup 的所有实例必须订阅完全相同的 topic 集合**，因此在正确使用场景下，"group 下所有 client" == "该 topic 下所有 client"，框架未提供更细粒度的查询。

---

## 方案：通过 ConsumerIdsChangeListener 自建 topic → clientInfo 索引

### 原理

`ConsumerManager.registerConsumer()` 在新 Channel 接入时会触发 `CLIENT_REGISTER` 事件，args 携带了 `ClientChannelInfo` 和订阅的 topic 集合，天然具备 client ↔ topic 的关联关系：

```java
// ConsumerManager.java:252
callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_REGISTER, group,
    clientChannelInfo,
    subList.stream().map(SubscriptionData::getTopic).collect(Collectors.toSet()));
// args[0] = ClientChannelInfo
// args[1] = Set<String> (topic 集合)
```

### 注册入口

`MessagingProcessor` 接口提供了正式的注册方法：

```java
// MessagingProcessor.java
void registerConsumerListener(ConsumerIdsChangeListener consumerIdsChangeListener);

// DefaultMessagingProcessor.java:362 实现
@Override
public void registerConsumerListener(ConsumerIdsChangeListener listener) {
    this.clientProcessor.registerConsumerIdsChangeListener(listener);
    // → serviceManager.getConsumerManager().appendConsumerIdsChangeListener(listener)
}
```

**注意**：RocketMQ Proxy 没有 SPI 自动发现机制，必须在 `messagingProcessor.start()` 之前显式调用。

### 实现代码

```java
public class TopicClientInfoIndex implements ConsumerIdsChangeListener {

    // Map<topic, Map<group, Set<clientId>>>
    private final ConcurrentMap<String, ConcurrentMap<String, Set<String>>> index =
        new ConcurrentHashMap<>();

    @Override
    public void handle(ConsumerGroupEvent event, String group, Object... args) {
        switch (event) {
            case CLIENT_REGISTER: {
                ClientChannelInfo info = (ClientChannelInfo) args[0];
                @SuppressWarnings("unchecked")
                Set<String> topics = (Set<String>) args[1];
                for (String topic : topics) {
                    index.computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                         .computeIfAbsent(group, k -> ConcurrentHashMap.newKeySet())
                         .add(info.getClientId());
                }
                break;
            }
            case CLIENT_UNREGISTER: {
                ClientChannelInfo info = (ClientChannelInfo) args[0];
                @SuppressWarnings("unchecked")
                Set<String> topics = (Set<String>) args[1];
                for (String topic : topics) {
                    Map<String, Set<String>> groupMap = index.get(topic);
                    if (groupMap != null) {
                        Set<String> clients = groupMap.get(group);
                        if (clients != null) {
                            clients.remove(info.getClientId());
                        }
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    @Override
    public void shutdown() {}

    /**
     * 查询 group + topic 下所有 clientId
     */
    public Set<String> getClientIds(String topic, String group) {
        Map<String, Set<String>> groupMap = index.get(topic);
        if (groupMap == null) return Collections.emptySet();
        Set<String> clients = groupMap.get(group);
        return clients != null ? Collections.unmodifiableSet(clients) : Collections.emptySet();
    }

    /**
     * 查询 topic 下所有 group 的 clientId（跨 group）
     */
    public Map<String, Set<String>> getClientIdsByTopic(String topic) {
        Map<String, Set<String>> groupMap = index.get(topic);
        return groupMap != null ? Collections.unmodifiableMap(groupMap) : Collections.emptyMap();
    }
}
```

### 启动时注册

```java
// 在 ProxyStartup 或自定义启动类中
DefaultMessagingProcessor messagingProcessor = DefaultMessagingProcessor.createForClusterMode();

TopicClientInfoIndex topicClientInfoIndex = new TopicClientInfoIndex();
messagingProcessor.registerConsumerListener(topicClientInfoIndex);  // start() 之前注册

messagingProcessor.start();
```

---

## 调用链全貌

```
Consumer 心跳 / 注册
  └─ ClusterConsumerManager.registerConsumer()       // proxy/service/client/ClusterConsumerManager.java:46
       ├─ heartbeatSyncer.onConsumerRegister()        // 广播到其他 Proxy 节点
       └─ super.registerConsumer()                    // broker/client/ConsumerManager.java:227
            ├─ consumerGroupInfo.updateChannel()      // 更新 channelInfoTable
            ├─ consumerGroupInfo.updateSubscription() // 更新 subscriptionTable
            └─ callConsumerIdsChangeListener(CLIENT_REGISTER, group, clientInfo, topics)
                 └─ TopicClientInfoIndex.handle()     // 写入自建索引
```

---

## 数据一致性说明

| 场景 | 处理方式 |
|------|---------|
| Client 正常注销 | `CLIENT_UNREGISTER` 事件触发，从索引中移除 |
| Channel 断开（网络中断） | `doChannelCloseEvent()` → `CLIENT_UNREGISTER` 事件，同样触发清理 |
| 同 group 订阅变更（心跳更新） | `CLIENT_REGISTER` 覆盖写，旧 topic 在 UNREGISTER 时清理；若订阅缩减，需依赖下次 UNREGISTER 清理旧条目 |
| HeartbeatSyncer 同步远端 Channel | 远端 Channel 注册时 `isNotifyConsumerIdsChangedEnable=false`，但仍会触发 `REGISTER` 事件（非 `CLIENT_REGISTER`），索引不受影响 |

---

## 替代方案对比

| 方案 | 说明 | 缺点 |
|------|------|------|
| `topicGroupTable` + `getChannelInfoTable()` | 框架原生，O(1) 查 group，再拿全部 client | 无法按 topic 过滤（同 group 所有 client 都返回） |
| 自建 `ConsumerIdsChangeListener` 索引 | 事件驱动，精确维护 topic→clientId 映射 | 需要手动注册，增量维护有一定复杂度 |
| 不同 group 隔离 | 每个 topic 用独立 group，彻底避免问题 | 改变消费模型，影响负载均衡语义 |
