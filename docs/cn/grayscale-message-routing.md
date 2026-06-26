# 灰度消息动态路由设计方案

## 1. 背景与需求

### 场景

在 pop 消费模式下，实现基于消息属性的灰度路由：

- Producer 发送消息时携带流量标（如 `trafficTag=gray-v2`）
- 隔离环境的消费者组只消费带特定流量标的消息
- 标准环境的消费者组消费不带流量标的消息
- 路由规则需支持运行时动态变更

### 核心约束

- 消费模式：pop 模式
- 灰度粒度：消息属性（trafficTag）
- 路由依据：流量标是否存在、隔离组是否存活
- 动态性：路由规则支持配置中心热推

## 2. 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                     GrayscaleRouter                      │
│  (路由规则管理 + 灰度组存活检测 + 过滤逻辑注入)          │
└──────────┬────────────────────────────────────┬──────────┘
           │                                    │
           ▼                                    ▼
┌─────────────────────────┐    ┌──────────────────────────────┐
│   PopMessageProcessor   │    │  GrayscaleMessageMigrator    │
│   (pop 时动态注入过滤)   │    │  (灰度组下线后迁移 orphan    │
│                          │    │   消息到标准组 retry topic)  │
└─────────────────────────┘    └──────────────────────────────┘
```

### 两条路径

| 路径 | 时机 | 行为 |
|------|------|------|
| **在线过滤** | 灰度组存活时 | PopMessageProcessor 根据流量标过滤消息，标准组跳过灰度消息，灰度组只收灰度消息 |
| **离线迁移** | 灰度组下线后 | GrayscaleMessageMigrator 扫描 gap 区间，将灰度消息写入标准组 retry topic，标准组自然消费 |

## 3. 组件设计

### 3.1 GrayscaleRoute

路由规则定义：

```java
public class GrayscaleRoute {
    private String topic;                // 原始 topic，如 "TopicA"
    private String standardConsumerGroup; // 标准消费组
    private String grayConsumerGroup;    // 灰度消费组，如 "GID_gray-v2"
    private String trafficTagKey;        // 流量标属性名，如 "TRAFFIC_TAG"
    private String trafficTagValue;      // 流量标属性值，如 "gray-v2"
    private volatile long lastGrayActiveTime; // 灰度组最后活跃时间戳
}
```

配置格式（JSON，可从 Nacos/Apollo 推送）：

```json
{
  "routes": [
    {
      "topic": "TopicA",
      "standardConsumerGroup": "GID_standard",
      "grayConsumerGroup": "GID_gray_v2",
      "trafficTagKey": "TRAFFIC_TAG",
      "trafficTagValue": "gray-v2"
    },
    {
      "topic": "TopicB",
      "standardConsumerGroup": "GID_standard",
      "grayConsumerGroup": "GID_gray_v3",
      "trafficTagKey": "TRAFFIC_TAG",
      "trafficTagValue": "gray-v3"
    }
  ]
}
```

### 3.2 GrayscaleRouter

路由规则管理器，负责规则加载、灰度组存活检测、过滤决策。

```java
public class GrayscaleRouter {
    private final Map<String, List<GrayscaleRoute>> topicRoutes; // topic → routes
    private final ConsumerManager consumerManager;
    private final ConsumerOffsetManager consumerOffsetManager;

    // 灰度组离线判定阈值
    private static final long GRAY_OFFLINE_THRESHOLD_MS = 30_000;

    /**
     * 灰度组是否有活跃消费者。
     * 通过 ConsumerManager 检查是否有注册的 channel，同时兜底检查最后活跃时间。
     */
    public boolean isGrayGroupActive(GrayscaleRoute route);

    /**
     * topic 是否有活跃的灰度路由（即有灰度组在消费）。
     * 用于判断标准组是否需要启用过滤。
     */
    public boolean hasActiveGrayRoute(String topic);

    /**
     * 判断当前请求的消费组是否是标准组，且需要过滤。
     */
    public boolean shouldFilterForStandardGroup(String topic, String consumerGroup);

    /**
     * 判断当前请求的消费组是否是灰度组。
     */
    public boolean isGrayGroup(String topic, String consumerGroup);

    /**
     * 获取灰度组在当前 queue 上的最后 offset。
     * 用于标准组 offset 回退（方案一）和 migrator 扫描（方案三）。
     */
    public long getGrayGroupOffset(String topic, String grayGroup, int queueId);
}
```

### 3.3 PopMessageProcessor 修改

在 pop 请求处理中注入灰度过滤逻辑。

#### 3.3.1 标准组过滤

标准组 pop 时，如果有活跃灰度组，需要过滤掉带活跃流量标的消息。

```java
// PopMessageProcessor.processRequest() 中，构建 messageFilter 之后
// 约第 377 行

if (grayscaleRouter.shouldFilterForStandardGroup(topic, consumerGroup)) {
    // 构建排除 SQL92 表达式
    // grayTag NOT IN ('gray-v2', 'gray-v3')
    String excludeExpr = buildExcludeExpression(
        grayscaleRouter.getActiveGrayTags(topic));
    messageFilter = new ExpressionMessageFilter(
        FilterAPI.build(topic, excludeExpr, ExpressionType.SQL92),
        null, brokerController.getConsumerFilterManager());
}
```

#### 3.3.2 灰度组过滤

灰度组 pop 时，只返回匹配流量标的消息。

```java
if (grayscaleRouter.isGrayGroup(topic, consumerGroup)) {
    String grayTagValue = grayscaleRouter.getGrayTagValue(topic, consumerGroup);
    // grayTag = 'gray-v2'
    String includeExpr = buildIncludeExpression(grayTagValue);
    messageFilter = new ExpressionMessageFilter(
        FilterAPI.build(topic, includeExpr, ExpressionType.SQL92),
        null, brokerController.getConsumerFilterManager());
}
```

#### 3.3.3 过滤与 offset 推进

```
标准组 pop 读取:  [M1(普通), M2(gray), M3(普通), M4(gray)]
                         ↓ SQL92 过滤 (NOT grayTag IN ('gray-v2'))
标准组收到:       [M1(普通), M3(普通)]
标准组 offset:    [4]  ← 越过 M2, M4（这两个留在队列中供灰度组消费）

灰度组 pop 读取:  [M1(普通), M2(gray), M3(普通), M4(gray)]
                         ↓ SQL92 过滤 (grayTag = 'gray-v2')
灰度组收到:       [M2(gray), M4(gray)]
灰度组 offset:    [4]
```

### 3.4 GrayscaleMessageMigrator

后台线程，灰度组下线后扫描 orphan 灰度消息，迁移到标准组 retry topic。

#### 3.4.1 触发条件

- 灰度组无活跃消费者超过 `GRAY_OFFLINE_THRESHOLD_MS`（默认 30s）
- 灰度组 offset < 标准组 offset（有未消费的 gap）

#### 3.4.2 扫描逻辑

```
灰度组 offset: 2
标准组 offset: 100

gap: [2, 100)
     ↓ 逐条读取，按 trafficTag 过滤
     ↓ 匹配的灰度消息写入标准组 retry topic
     ↓ 灰度组 offset 推进到 100
```

```java
public class GrayscaleMessageMigrator extends ServiceThread {
    private static final long SCAN_INTERVAL_MS = 10_000;
    // (topic@queueId@grayGroup) → 已迁移到的 offset
    private final ConcurrentMap<String, Long> migratedOffsetTable;

    @Override
    public void run() {
        while (!isStopped()) {
            for (GrayscaleRoute route : grayscaleRouter.getRoutes()) {
                processRoute(route);
            }
            waitForRunning(SCAN_INTERVAL_MS);
        }
    }

    private void processRoute(GrayscaleRoute route) {
        // 灰度组还活着 → 跳过
        if (grayscaleRouter.isGrayGroupActive(route)) return;

        TopicConfig topicConfig = getTopicConfig(route.getTopic());
        for (int queueId = 0; queueId < topicConfig.getReadQueueNums(); queueId++) {
            migrateQueue(route, queueId);
        }
    }

    private void migrateQueue(GrayscaleRoute route, int queueId) {
        String topic = route.getTopic();
        String grayGroup = route.getGrayConsumerGroup();
        String stdGroup = route.getStandardConsumerGroup();

        long grayOffset = consumerOffsetManager.queryOffset(grayGroup, topic, queueId);
        long stdOffset = consumerOffsetManager.queryOffset(stdGroup, topic, queueId);
        if (grayOffset < 0 || stdOffset < 0 || grayOffset >= stdOffset) return;

        String migrateKey = topic + "@" + queueId + "@" + grayGroup;
        long migrated = migratedOffsetTable.getOrDefault(migrateKey, grayOffset);
        if (migrated >= stdOffset) return;

        // 扫描 [migrated, stdOffset) 区间
        long nextOffset = scanAndMigrate(route, queueId, migrated, stdOffset);

        // 推进灰度组 offset + 记录已迁移位置
        consumerOffsetManager.commitOffset(
            "grayscale-migrator", grayGroup, topic, queueId, nextOffset);
        migratedOffsetTable.put(migrateKey, nextOffset);
    }
}
```

#### 3.4.3 消息写入 retry topic

参照 `PopReviveService.reviveRetry()` 的消息格式：

```java
private void writeToRetry(MessageExt msg, GrayscaleRoute route) {
    MessageExtBrokerInner retryMsg = new MessageExtBrokerInner();

    String retryTopic = KeyBuilder.buildPopRetryTopic(
        route.getTopic(), route.getStandardConsumerGroup(),
        brokerController.getBrokerConfig().isEnableRetryTopicV2());
    retryMsg.setTopic(retryTopic);
    retryMsg.setBody(msg.getBody());
    retryMsg.setTags(msg.getTags());
    retryMsg.setBornTimestamp(msg.getBornTimestamp());
    retryMsg.setBornHost(brokerController.getStoreHost());
    retryMsg.setStoreHost(brokerController.getStoreHost());
    retryMsg.setReconsumeTimes(0);

    // 保留原始属性 + 标记来源
    retryMsg.getProperties().putAll(msg.getProperties());
    retryMsg.getProperties().put(MessageConst.PROPERTY_ORIGIN_GROUP,
        route.getGrayConsumerGroup());
    retryMsg.getProperties().put(MessageConst.PROPERTY_FIRST_POP_TIME,
        String.valueOf(System.currentTimeMillis()));
    retryMsg.setPropertiesString(
        MessageDecoder.messageProperties2String(retryMsg.getProperties()));

    retryMsg.setQueueId(
        getRetryQueueId(retryTopic, route.getStandardConsumerGroup(), msg));
    addRetryTopicIfNotExist(retryTopic, route.getStandardConsumerGroup());

    escapeBridge.putMessageToSpecificQueue(retryMsg);
}
```

#### 3.4.4 标准组消费 retry topic 的时机

标准组 pop 时，broker 会按 `popFromRetryProbability` 概率（默认约 50%）从 retry topic 取消息。迁移的消息不需要额外触发，自然被标准组消费到。

```
标准组 pop 请求
       │
       ├── (概率 ~50%) 从 retry topic pop → 拿到迁移的灰度消息 → 返回给消费者
       │
       └── (概率 ~50%) 从原 topic pop → 正常消息
```

## 4. 数据流全貌

### 4.1 灰度组正常运行

```
Producer 发送 M1(普通), M2(gray), M3(普通), M4(gray) → Queue 0

标准组 pop:
  read offset=0, batch=32
  SQL92: grayTag NOT IN ('gray-v2')
  return [M1, M3]
  stdOffset += 4  →  4

灰度组 pop:
  read offset=0, batch=32
  SQL92: grayTag = 'gray-v2'
  return [M2, M4]
  grayOffset += 4  →  4
```

### 4.2 灰度组下线 → 迁移

```
灰度组消费到 offset=2 后挂了
  grayOffset = 2
  stdOffset = 100

GrayscaleMessageMigrator 扫描:
  Queue 0, range [2, 100), 找到 M6(gray), M18(gray), ...
                                  ↓
                          写入标准组 retry topic
                                  ↓
  grayOffset 推进到 100
  migratedOffset 记录到 100
```

### 4.3 灰度组重新上线

```
灰度组重新连接 → isGrayGroupActive() = true
  → migrator 停止迁移该路由
  → grayOffset 已推进到 100（被 migrator 推进）
  → 灰度组从 offset=100 开始消费新消息
  → 已迁移的消息已被标准组消费
```

## 5. 边界情况处理

| 场景 | 处理方式 |
|------|---------|
| **灰度组存活但无消息** | `isGrayGroupActive()=true`，PopMessageProcessor 正常过滤，migrator 跳过 |
| **灰度组短暂闪断（<30s）** | 未达到 `GRAY_OFFLINE_THRESHOLD`，migrator 不触发 |
| **迁移中途灰度组上线** | 下次扫描 `isGrayGroupActive()=true`，停止迁移。已写入 retry topic 的消息由标准组消费，灰度组从已推进的 offset 继续 |
| **灰度组永久下线** | 一次或多次扫描完成全部 gap 迁移，`grayOffset == stdOffset` 后不再处理 |
| **灰度组 offset 未初始化（= -1）** | 跳过，等待灰度组至少消费一次后再处理 |
| **消息已被 CommitLog 删除** | `GetMessageStatus.MESSAGE_WAS_REMOVING`，跳过，推进 offset |
| **标准组 retry topic 不存在** | `addRetryTopicIfNotExist()` 自动创建 |
| **多次迁移重复** | `migratedOffsetTable` + `grayOffset` 双重保证幂等 |
| **标准组 offset 回退** | 不影响，migrator 按当前 offset 扫描，只迁移 gap |
| **灰度组有多个 queue** | loop `topicConfig.getReadQueueNums()` 每个 queue 独立处理 |

## 6. 改动清单

| 文件 | 改动类型 | 说明 |
|------|---------|------|
| `broker/.../grayscale/GrayscaleRoute.java` | 新增 | 路由规则定义 |
| `broker/.../grayscale/GrayscaleRouter.java` | 新增 | 规则管理 + 存活检测 + 过滤决策 |
| `broker/.../grayscale/GrayscaleMessageMigrator.java` | 新增 | 灰度消息离线迁移后台线程 |
| `broker/.../processor/PopMessageProcessor.java` | 修改 | pop 请求中注入灰度过滤 |
| `broker/.../BrokerController.java` | 修改 | 启动 GrayscaleRouter 和 GrayscaleMessageMigrator |
| `broker/.../grayscale/GrayscaleConfig.java` | 新增 | 灰度路由配置（下线阈值、扫描周期等） |

总计约 400-500 行新增代码。
