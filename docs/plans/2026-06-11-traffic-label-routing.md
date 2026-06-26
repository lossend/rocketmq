# 流量标动态路由实现计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**目标：** 基于消息流量标实现动态消费路由：隔离环境 Producer 发送带标消息；如果同一消费组内存在相同隔离标的在线隔离消费者，则由该隔离环境消费；否则回退到标准环境消费。

**架构：** Topic 和 consumer group 维持同一套逻辑资源，不为每个隔离环境拆 Topic 或拆组。Java 5.x Client 负责把 Producer 消息流量标和 Consumer 环境流量标传到 Proxy；多 Proxy 下由 Proxy 集群维护 `group/topic/trafficLabel` 在线视图，并在 POP 请求中携带快照；Broker 负责最终路由判断，因为 offset、POP 不可见时间、重试、DLQ、FIFO 顺序都在 Broker 侧闭环。非 FIFO POP 通过持久化 deferred ledger 跳过暂时不属于当前消费者的消息，避免阻塞后续标准消息；FIFO 保持全局队列顺序，队头消息不属于当前消费者时只能阻塞。

**技术栈：** RocketMQ Java 5.x Client、RocketMQ Proxy gRPC v2、Broker POP 消费链路、remoting header、Proxy heartbeat syncer、Broker commitlog-backed 内部主题或等价 HA 持久化存储。

---

## 已确认语义

- 标准环境的 `trafficLabel` 为空字符串。
- 隔离环境的 `trafficLabel` 为非空字符串，例如 `gray1`。
- 隔离 Producer 发送消息时写入消息属性：`__RMQ_TRAFFIC_LABEL=<label>`。
- 隔离 Consumer 通过客户端配置声明自身环境：`trafficLabel=<label>`。
- 无流量标消息只由标准环境 Consumer 消费。
- 有流量标消息在同一逻辑 consumer group 内存在相同在线隔离 Consumer 时，由对应隔离 Consumer 消费。
- 如果消费时没有相同流量标的在线隔离 Consumer，有流量标消息回退给标准环境 Consumer。
- v1 范围覆盖 Java 5.x + Proxy。未携带 consumer label 的旧 remoting 客户端按标准环境处理。
- 非 FIFO 消费允许标准消息越过当前被隔离环境占用的有标消息。
- FIFO 消费必须保持全局队列顺序。如果队头消息属于另一个在线隔离标，非 owner Consumer 返回空结果并等待 owner 消费或 owner 下线。

## 核心路由、跳过与重试语义

### 路由判定模型

Broker 在每次消息即将投递给 Consumer 前实时判定路由，不把某条消息永久绑定给某个环境。判定输入包括：

- 消息上的 `messageTrafficLabel`，来自 `__RMQ_TRAFFIC_LABEL`。
- 当前拉取请求上的 `consumerTrafficLabel`，来自 `PopMessageRequestHeader.trafficLabel` 或 `PopLiteMessageRequestHeader.trafficLabel`。
- 当前 Broker 可用的在线 Consumer label 集合。Java 5.x + Proxy 路径优先使用 POP 请求中由 Proxy 携带的集群在线 label 快照；直连 remoting 路径才使用 Broker 本地 `ConsumerManager` 可见的活跃 Consumer 连接。

判定规则：

```text
if messageTrafficLabel is blank:
    only standard consumer is eligible
else if consumerTrafficLabel == messageTrafficLabel:
    matching isolation consumer is eligible
else if consumerTrafficLabel is blank and no online consumer has messageTrafficLabel:
    standard consumer is eligible by fallback
else:
    current consumer is not eligible
```

这里的“没有在线隔离 Consumer”必须以新鲜的集群在线 label 视图为准。隔离 Consumer 进程突然退出时，系统需要等 channel close、unregister 或 heartbeat/lease 过期后才认为它离线；在这之前仍按隔离 Consumer 在线处理。

### Broker 如何判断某个 label 在线

Broker 不主动扫描全量 Consumer，也不假设自己能看到所有 Consumer。Broker 只在单次 POP 路由决策里判断“本次请求上下文中，`gray1` 是否在线”。

Java 5.x + Proxy 路径的判断来源：

1. Consumer 连接到某个 Proxy 时，Proxy 从 `x-mq-traffic-label` 得到 Consumer 环境 label。
2. Proxy 通过 `TrafficLabelPresenceManager` 维护本地和远端 Proxy 同步来的 Consumer lease。
3. Proxy 发起 POP 请求 Broker 前，按 `consumerGroup/topic` 查询 presence manager，生成在线 label 快照：
   - `onlineTrafficLabels`
   - `trafficLabelSnapshotTimestamp`
   - `trafficLabelSnapshotVersion`
4. Broker 收到 POP 请求后先校验快照是否可用：
   - `onlineTrafficLabels` 字段存在。
   - `trafficLabelSnapshotTimestamp` 未超过 `trafficLabelRoutingMaxSnapshotAgeMs`。
   - snapshot version 可用于日志和排查。
5. 快照可用时，Broker 用 `onlineTrafficLabels.contains(messageTrafficLabel)` 判断该 label 是否在线。
6. 快照缺失或过期时，状态为 `UNKNOWN`，标准 Consumer 不允许 fallback；非 FIFO 走 `DEFER`，FIFO 走 `BLOCK`。

伪代码：

```text
labelState(messageTrafficLabel, routeContext):
    if messageTrafficLabel is blank:
        return OFFLINE

    if routeContext.source == PROXY_SNAPSHOT:
        if routeContext.snapshotMissingOrExpired():
            return UNKNOWN
        if routeContext.onlineTrafficLabels contains messageTrafficLabel:
            return ONLINE
        return OFFLINE

    if routeContext.source == BROKER_LOCAL:
        if consumerManager.hasConsumerWithTrafficLabel(group, topic, messageTrafficLabel):
            return ONLINE
        return OFFLINE

    return UNKNOWN
```

直连 remoting 路径没有 Proxy 集群快照，只能使用 Broker 本地 `ConsumerManager#hasConsumerWithTrafficLabel(group, topic, label)`。这条路径无法天然覆盖“Consumer 连到其他 Proxy”的情况，所以 Java 5.x + Proxy v1 必须走 `PROXY_SNAPSHOT`。

### 标准 Consumer 遇到有标消息时怎么处理

当标准环境 Consumer 拉取到 `gray1` 消息，且 Broker 基于本次请求携带的 Proxy 集群在线 label 快照判断 `gray1` 隔离 Consumer 在线：

- 非 FIFO POP：标准 Consumer 不消费这条消息，但也不能简单跳过并丢掉可达性。Broker 必须先把该消息写入持久化 deferred ledger，记录 `group/topic/queueId/queueOffset/commitLogOffset/messageSize/messageTrafficLabel`，写入成功后才允许当前 POP 扫描继续向后找后续可消费消息。
- FIFO POP：标准 Consumer 不能跳过队头消息，也不创建 deferred record；如果队头是 `gray1` 且 `gray1` 隔离 Consumer 在线，当前 queue 返回空结果，等待匹配隔离 Consumer 消费或等待该 label 离线后标准 Consumer 再消费。

因此，非 FIFO 下“跳过”是“持久化挂起并继续扫描”，不是 commit 成功，也不是删除消息；FIFO 下“不跳过”，而是阻塞队列头。

### deferred 消息如何重新被消费

deferred ledger 是非 FIFO 场景下被跳过消息的二级待投递索引。每次 POP 扫描正常 consume queue 前，Broker 先查询当前 Consumer 可消费的 deferred record，并基于本次 POP 请求携带的在线 label 快照重新判定：

- `gray1` 隔离 Consumer 在线并发起 POP：优先拿到 `gray1` deferred 消息。
- `gray1` 隔离 Consumer 已经全部离线，标准 Consumer 发起 POP：标准 Consumer 可以拿到这些 `gray1` deferred 消息。
- `gray1` 隔离 Consumer 又重新上线：后续还未投递的 `gray1` deferred 消息重新优先给 `gray1` 隔离 Consumer。

隔离 Consumer 消费 deferred 消息的流程：

1. `gray1` Consumer 发起 POP，请求携带 `trafficLabel=gray1`。
2. Broker 在扫描正常 consume queue 前，先查本地 deferred index，条件是 `group/topic/queueId` 匹配且 `messageTrafficLabel == gray1`。
3. 命中的 deferred record 通过 `commitLogOffset/messageSize` 读取原始消息体，并重新执行 subscription filter。
4. filter 通过后，把消息放入现有 POP in-flight/checkpoint 流程，返回给 `gray1` Consumer。
5. record 被成功放入 POP in-flight/checkpoint 后，写删除 tombstone 或删除 deferred record。
6. 如果 `gray1` Consumer 未 ack，后续仍按 POP invisible/revive/retry 恢复；恢复后再次按最新在线 label 快照判路由。

标准 Consumer 不能抢走仍有在线 owner 的 deferred 消息：标准 Consumer 查询 deferred 时必须检查 `onlineTrafficLabels`，只允许返回“当前快照中没有对应 `messageTrafficLabel` owner”的 record。

deferred record 只负责保存“原始 consume queue 已经被正常 POP cursor 越过，但消息还没有被成功交给某个 eligible Consumer”的状态。消息一旦被放入现有 POP in-flight/checkpoint 流程，deferred record 可以删除，后续失败恢复交给 RocketMQ 现有 POP 不可见时间和 revive/retry 机制。

### Deferred Ledger 数据结构和高性能查询

deferred ledger 分两层：commitlog-backed 内部主题保存源数据，本地 index/cache 负责高频查询。不要在 POP 请求里扫描内部主题或全量 deferred record。

源数据 record：

```text
key = brokerName/group/topic/queueId/queueOffset
value = {
    brokerName,
    group,
    topic,
    queueId,
    queueOffset,
    commitLogOffset,
    messageSize,
    messageTrafficLabel,
    storeTimestamp,
    state: ACTIVE | TOMBSTONE
}
```

本地 index 至少维护两类 key：

```text
byLabel:
  brokerName/group/topic/messageTrafficLabel/queueId/queueOffset -> recordKey

byQueue:
  brokerName/group/topic/queueId/queueOffset -> recordKey
```

多隔离环境下，隔离 Consumer 查询只走 `byLabel` 前缀，不扫其他 label：

```text
pollDeferredForIsolation(group, topic, queueId, consumerLabel, maxNum):
    prefix = brokerName/group/topic/consumerLabel/queueId
    return first maxNum ACTIVE records ordered by queueOffset
```

标准 Consumer 查询不能扫所有 deferred label。实现上用本次请求的 `onlineTrafficLabels` 做差集过滤：

```text
pollDeferredForStandard(group, topic, queueId, onlineTrafficLabels, maxNum):
    scan byQueue prefix brokerName/group/topic/queueId ordered by queueOffset
    return first maxNum ACTIVE records where messageTrafficLabel not in onlineTrafficLabels
```

`byQueue` 可能包含多个隔离 label，但它只按当前 queue 的 deferred backlog 顺序扫描，并受 `trafficLabelDeferredMaxScanPerPop` 限制。这样标准 Consumer 不需要按 label 建 N 个查询，也不会扫全 Broker。

性能约束：

- 隔离 Consumer 查询复杂度约为 `O(resultSize)`，因为 label 已在 key 前缀里。
- 标准 Consumer 查询复杂度约为 `O(scannedInQueue)`，上限由 `trafficLabelDeferredMaxScanPerPop` 控制。
- `onlineTrafficLabels` 应在 Broker 收到请求时解析为 `HashSet`，避免每条 record 做线性 contains。
- record 删除使用 tombstone 写入内部主题，再异步清理本地 index；POP 热路径只做幂等状态检查。
- 每个 `group/topic/queueId` 可以维护一个 round-robin 或 last-scan cursor，避免大量不 eligible 的 deferred record 长期压在头部导致重复扫描。
- 如果某个 queue 的 deferred backlog 超过阈值，输出限频日志和 metrics，不自动扩大扫描上限。

### 隔离 Consumer 消费中突然下线且未 ack

如果 `gray1` 隔离 Consumer 已经拿到 `gray1` 消息，但在 ack 前进程退出或网络断开：

1. 在 POP invisible time 未到期前，这条消息仍处于 in-flight 状态，标准 Consumer 不能立即消费它，否则会破坏 POP 的不可见时间和 at-least-once 语义。
2. invisible time 到期后，现有 POP revive/retry 机制让该消息重新变为可投递状态。
3. 重新投递时必须再次执行流量标路由判定，而不是沿用上一次投递给隔离 Consumer 的结果。
4. 如果此时 Broker 已经确认没有 `gray1` 隔离 Consumer 在线，标准 Consumer 可以通过 fallback 消费这条消息。
5. 如果 `gray1` 隔离 Consumer 在重试投递前已经重新上线，则消息仍优先由 `gray1` 隔离 Consumer 消费。

这意味着路由归属是“每次可见、每次投递时动态计算”的，不是“第一次匹配后永久归属隔离环境”。

### retry 与 DLQ 路由规则

- retry/revive 后的消息必须保留原始 `__RMQ_TRAFFIC_LABEL` 属性。
- 每次从 POP revive/retry 路径重新投递前，都按上面的同一套路由规则重新计算。
- 重试次数、不可见时间、ack、change invisible、进入 DLQ 的条件仍沿用现有 RocketMQ 语义。
- 消息进入 DLQ 后不再为正常消费组做动态回退；DLQ 消费按现有 DLQ 主题消费语义处理，但保留 `__RMQ_TRAFFIC_LABEL` 便于排查。

## 分布式部署语义

### 多 Proxy

Java 5.x Consumer 连接的是 Proxy，不是 Broker。多 Proxy 部署时，Broker 不能只依赖本地 `ConsumerManager` 判断某个隔离 label 是否在线，否则会出现：

- `gray1` Consumer 连接在 Proxy A。
- 标准 Consumer 连接在 Proxy B。
- Proxy B 向 Broker 拉取 `gray1` 消息时，如果 Broker 或 Proxy B 不知道 Proxy A 上的 `gray1` Consumer，就可能错误 fallback 给标准环境。

因此 v1 的严格语义必须让 Proxy 集群维护全局在线 label 视图：

1. 在 Proxy 增加 `TrafficLabelPresenceManager`，按 `consumerGroup/topic/trafficLabel/proxyId/clientId/channelId` 维护在线 lease。
2. 扩展 `HeartbeatSyncerData`，在 register/unregister 广播中携带 `trafficLabel` 和订阅 topic 集合。
3. 每个 Proxy 接收其他 Proxy 的 heartbeat sync 消息后，把远端 Consumer 注册到本地 presence manager。
4. 每个 lease 有过期时间，例如 `trafficLabelPresenceTtlMs`。未收到 unregister 时，通过 TTL 清理，避免进程崩溃后永久认为隔离环境在线。
5. Proxy 在每次 Receive/POP 请求 Broker 时携带：
   - 当前 Consumer 的 `trafficLabel`。
   - 当前 `consumerGroup/topic` 下 Proxy 集群认为在线的非空 `onlineTrafficLabels`。
   - presence snapshot timestamp 或 version，用于 Broker 判断状态是否过期。
6. Broker 的路由判断优先使用请求携带的 `onlineTrafficLabels`。只有直连 remoting 客户端或没有 Proxy snapshot 时，才退回 Broker 本地 `ConsumerManager`。

默认策略必须 fail closed：如果标准 Consumer 请求携带的在线 label 快照缺失、过期或不可判定，Broker 不应该把有标消息 fallback 给标准环境。非 FIFO 下应 `DEFER`，FIFO 下应 `BLOCK`。这样会牺牲短时间可用性，但避免隔离 Consumer 实际在线时被标准环境误消费。

### 多 Broker master

多 master 下，Topic 的不同 queue 分布在不同 Broker master。路由原则是：

1. 每个 Broker master 只对自己本地 queue 上的消息做投递、deferred、in-flight、retry、DLQ 处理。
2. “某个隔离 label 是否在线”是 `consumerGroup/topic` 维度的集群状态，不是单个 Broker 本地状态。
3. 同一个标准 Consumer 通过任意 Proxy 拉取任意 Broker master 时，请求中都应携带同一份 Proxy 集群在线 label 视图。
4. Broker 之间不需要互相转移消息，也不需要跨 Broker 查 deferred record；每个 Broker 只维护本地 queue 的 deferred ledger。
5. deferred record 的 key 至少包含 `brokerName/topic/group/queueId/queueOffset`，避免诊断和迁移时与其他 master 的同名 queue 混淆。

多 master 场景下最关键的正确性约束是 deferred ledger 的持久化域。非 FIFO 下 Broker 只有在 deferred record 持久化成功后才能推进 POP cursor 越过该消息。如果 deferred record 只写在未复制的本地 RocksDB，而 consumer offset 或其他状态在主从切换后已经推进，就可能导致这条被跳过的消息失去可达性。

生产实现必须满足以下之一：

- 首选：把 deferred record 写入 commitlog-backed Broker 内部主题，例如 `RMQ_SYS_TRAFFIC_LABEL_DEFERRED_TOPIC`，利用 Broker 现有 HA/复制能力保证与消息存储同一故障域；Broker 启动或 master 切换后由该内部主题重建本地 deferred index。
- 可选：RocksDB 只作为本地查询索引/cache，源数据仍以 commitlog-backed 内部主题为准。
- 不推荐：只使用本地 RocksDB 作为唯一 deferred 存储。除非明确关闭主从切换场景，或证明 RocksDB 与 consumer offset 推进具备同等复制和恢复保证。

### 状态一致性和降级

- 隔离 Consumer 上线不是瞬时全局可见，要等 Proxy register、heartbeat sync、presence manager 更新完成。
- 隔离 Consumer 下线也不是瞬时 fallback，要等 unregister 广播或 TTL 过期。
- Proxy 间同步中断时，标准环境对有标消息默认不 fallback，而是 defer/block，直到状态恢复或 lease 过期。
- 如果业务更关注可用性而不是隔离严格性，可以增加显式配置 `trafficLabelRoutingFallbackOnStateUnknown=false`，默认必须为 `false`。
- 所有涉及在线状态的日志和指标都应包含 `snapshotAgeMs`、`snapshotVersion` 或等价诊断字段，方便排查多 Proxy 状态不同步。

## 按消息类型和队列类型的实现策略

### 普通消息

普通消息直接写入真实业务 Topic 的 consume queue。流量标路由发生在 Broker 处理 POP 拉取时：

1. `PopMessageProcessor` 解出 `PopMessageRequestHeader.trafficLabel`。
2. `PopConsumerService` 或 POP 扫描逻辑读取候选消息属性 `__RMQ_TRAFFIC_LABEL`。
3. 订阅过滤通过后、构造 POP response 前调用 `TrafficLabelRouteManager`。
4. 路由结果为 `DELIVER` 时，消息进入现有 POP response、checkpoint 和 invisible time 流程。
5. 路由结果为 `DEFER` 时，先写 deferred ledger，再继续扫描后续消息。
6. 路由结果为 `BLOCK` 时，当前 queue 本轮不返回消息。

普通消息的非 FIFO 队列允许通过 deferred ledger 让标准消息越过被隔离环境占用的有标消息。deferred record 不是消费成功记录，不能代替 ack，也不能提前删除原消息的失败恢复能力。

### FIFO 消息

FIFO 消息仍使用真实业务 Topic 的 FIFO queue，不能为了流量路由破坏 queue 内顺序。实现规则：

1. 对 FIFO queue 只对队头候选消息做路由判断。
2. 队头无 label 时，只允许标准 Consumer 消费。
3. 队头 label 与当前隔离 Consumer label 相同，允许该隔离 Consumer 消费。
4. 队头 label 有在线 owner，而当前 Consumer 不是 owner，返回 `BLOCK`，不推进 offset，不创建 deferred record。
5. 队头 label 没有在线 owner，标准 Consumer 可以 fallback 消费。
6. 隔离 Consumer 拿到 FIFO 消息但未 ack 下线时，仍等待 invisible time 到期；到期后重新判路由。

FIFO 的核心取舍是保证顺序优先于吞吐：标准环境不能越过队头 `gray1` 消息去消费后续无标消息，否则会破坏全局 FIFO 语义。

### 延迟消息和定时消息

延迟消息在到期前位于 RocketMQ 内部延迟/定时存储路径，不应该在内部 schedule queue 上做业务消费路由。实现规则：

1. Producer 写入 `__RMQ_TRAFFIC_LABEL` 后，该属性必须随消息一起进入延迟/定时内部存储。
2. `ScheduleMessageService` 到期恢复消息时，会清理 `PROPERTY_DELAY_TIME_LEVEL` 并把消息恢复到 `PROPERTY_REAL_TOPIC`；此过程必须保留 `__RMQ_TRAFFIC_LABEL`。
3. Timer 延迟属性，例如 `TIMER_DELAY_SEC`、`TIMER_DELAY_MS`、`TIMER_DELIVER_MS`，也必须在消息到期恢复到真实 Topic 后保留业务流量标。
4. 到期恢复到真实 Topic 后，消息才进入普通或 FIFO 的路由流程。
5. 如果延迟消息到期时 `gray1` Consumer 在线，后续 POP 优先投递给 `gray1`。
6. 如果到期时或后续消费时 `gray1` Consumer 已离线，标准 Consumer 可以 fallback。

因此，延迟消息的路由时机是“到期后变成真实 Topic 可消费消息时”，不是“写入 schedule topic 时”。内部 schedule offset 的推进不能被流量标路由阻塞。

### 事务消息和事务队列

事务消息在 commit 前位于事务半消息队列，业务 Consumer 不应该看见半消息，也不应该在半消息队列做动态消费路由。实现规则：

1. Producer 发送 transaction prepared message 时，如果消息带 `__RMQ_TRAFFIC_LABEL`，半消息必须保留该属性。
2. 半消息写入 `RMQ_SYS_TRANS_HALF_TOPIC` 或 RocksDB 事务半消息存储时，不触发业务流量路由。
3. Broker 事务回查 `CHECK_TRANSACTION_STATE` 面向 Producer，不携带 Consumer 环境 label，也不参与消费路由。
4. Producer commit 后，`EndTransactionProcessor` 或 `TransactionalMessageUtil.buildTransactionalMessageFromHalfMessage` 构造最终业务消息，恢复到 `PROPERTY_REAL_TOPIC`，必须保留 `__RMQ_TRAFFIC_LABEL`。
5. Producer rollback 后不产生业务可消费消息，也不会产生 deferred record。
6. commit 后写入真实 Topic 的最终消息，按普通或 FIFO 策略进行动态路由。
7. 事务 op topic 只记录事务内部删除/确认语义，不参与业务流量路由。

事务消息的路由时机是“commit 后最终消息进入真实 Topic 并对 Consumer 可见时”。半消息队列、op 队列、事务回查都只负责事务状态机，不负责标准/隔离环境消费选择。

## 对外接口

- 在 `common` 增加统一常量：
  - 消息属性：`__RMQ_TRAFFIC_LABEL`
  - gRPC metadata header：`x-mq-traffic-label`
  - Remoting 请求扩展字段：`trafficLabel`
- 不要把 `__RMQ_TRAFFIC_LABEL` 加入 `MessageConst.STRING_HASH_SET`，因为 Java Producer API 需要允许用户以 property 形式写入该属性。
- 在 `/Users/lossend/opensource/rocketmq-clients/java/client-apis` 增加 `MessageBuilder#setTrafficLabel(String trafficLabel)`。
  - 实现上写入 `__RMQ_TRAFFIC_LABEL`。
  - `null` 或 blank 表示无流量标，不写入该属性。
- 在 Java client 增加 `ClientConfigurationBuilder#setTrafficLabel(String trafficLabel)` 和 `ClientConfiguration#getTrafficLabel()`。
  - `null` 或 blank 表示标准环境。
  - `Signature.sign()` 在配置了非空 label 时写入 `x-mq-traffic-label`。
- 在 Proxy 增加 `ProxyContext#setTrafficLabel/getTrafficLabel`。
  - `ContextInitPipeline` 从 `x-mq-traffic-label` 读取并写入 `ProxyContext`。
- 在 remoting 对象中增加可选字段 `trafficLabel`：
  - `ConsumerData`：用于直连 remoting 和未来 Broker 侧 Consumer 注册。
  - `HeartbeatSyncerData`：用于多 Proxy 同步 Consumer 环境 label。
  - `PopMessageRequestHeader`、`PopLiteMessageRequestHeader`：用于每次 POP 请求携带当前 Consumer 环境。
- 在 POP request header 增加 Proxy 集群在线 label 快照字段：
  - `onlineTrafficLabels`：当前 `consumerGroup/topic` 下在线的非空隔离 label 列表，建议逗号分隔并限制 label 字符集为 `[A-Za-z0-9._-]`。
  - `trafficLabelSnapshotTimestamp`：Proxy 生成该快照的时间。
  - `trafficLabelSnapshotVersion`：Proxy presence manager 的单调递增版本，便于日志和排查。
- Broker 路由判断使用 `trafficLabel` + `onlineTrafficLabels`。标准 Consumer 只有在快照新鲜且不包含消息 label 时才 fallback。

## Broker 实现

### Task 1：增加流量标常量

**文件：**
- 修改：`common/src/main/java/org/apache/rocketmq/common/message/MessageConst.java`
- 修改：`common/src/main/java/org/apache/rocketmq/common/constant/GrpcConstants.java`

**步骤：**
1. 在 `MessageConst` 增加 `PROPERTY_TRAFFIC_LABEL = "__RMQ_TRAFFIC_LABEL"`。
2. 确认不把 `PROPERTY_TRAFFIC_LABEL` 加入 `STRING_HASH_SET`。
3. 在 `GrpcConstants` 增加 `TRAFFIC_LABEL = Metadata.Key.of("x-mq-traffic-label", Metadata.ASCII_STRING_MARSHALLER)`。
4. 增加 POP header 字段名常量：`onlineTrafficLabels`、`trafficLabelSnapshotTimestamp`、`trafficLabelSnapshotVersion`。
5. 增加单测，证明用户属性可以包含 `__RMQ_TRAFFIC_LABEL`。

### Task 2：跟踪 Consumer 环境流量标

**文件：**
- 修改：`remoting/src/main/java/org/apache/rocketmq/remoting/protocol/heartbeat/ConsumerData.java`
- 修改：`broker/src/main/java/org/apache/rocketmq/broker/client/ClientChannelInfo.java`
- 修改：`broker/src/main/java/org/apache/rocketmq/broker/client/ConsumerGroupInfo.java`
- 修改：`broker/src/main/java/org/apache/rocketmq/broker/client/ConsumerManager.java`

**步骤：**
1. 在 `ConsumerData` 增加 nullable `trafficLabel` 字段、getter/setter 和 `toString`。
2. 在 `ClientChannelInfo` 增加 nullable `trafficLabel` 字段。
3. 为 `ClientChannelInfo` 增加构造函数重载，避免一次性破坏现有调用方。
4. 在 `ConsumerGroupInfo` 中保存每个 channel 对应的 `trafficLabel`。
5. 在 `ConsumerManager` 增加 `hasConsumerWithTrafficLabel(group, topic, trafficLabel)`，用于直连 remoting 和测试场景：
   - blank label 直接返回 `false`。
   - 只统计活跃 channel。
   - 只统计订阅了对应 topic 的 Consumer。
6. unregister 和 channel close 时清理 label 状态。
7. 注意：Java 5.x + Proxy POP 路径不能只依赖 Broker 本地 `ConsumerManager`，必须优先使用 Proxy 请求携带的 `onlineTrafficLabels`。
8. 增加单测覆盖注册、更新、注销和 channel close。

### Task 3：增加 Broker 路由管理器

**文件：**
- 新增：`broker/src/main/java/org/apache/rocketmq/broker/routing/TrafficLabelRouteManager.java`
- 新增：`broker/src/main/java/org/apache/rocketmq/broker/routing/TrafficLabelRouteResult.java`
- 修改：`broker/src/main/java/org/apache/rocketmq/broker/BrokerController.java`
- 修改：`common/src/main/java/org/apache/rocketmq/common/BrokerConfig.java`

**步骤：**
1. 增加 Broker 配置：
   - `enableTrafficLabelRouting=false`
   - `trafficLabelRoutingMaxScanPerPop=1024`
   - `trafficLabelDeferredMaxScanPerPop=1024`
   - `trafficLabelRoutingMaxSnapshotAgeMs=30000`
   - `trafficLabelRoutingFallbackOnStateUnknown=false`
2. 在 `BrokerController` 初始化 `TrafficLabelRouteManager`。
3. 增加 `TrafficLabelRouteContext` 或等价参数对象，包含：
   - `consumerTrafficLabel`
   - `onlineTrafficLabels`
   - `snapshotTimestamp`
   - `snapshotVersion`
   - `source`：`PROXY_SNAPSHOT` 或 `BROKER_LOCAL`
4. 实现路由判断，并返回明确动作：
   - `DELIVER`：当前 Consumer 可以消费。
   - `DEFER`：非 FIFO 下当前 Consumer 不可消费，但可以持久化 deferred record 后继续扫描。
   - `BLOCK`：FIFO 下当前 Consumer 不可消费，当前 queue 返回空结果。
   - `UNKNOWN`：在线 label 状态缺失或过期，且当前 Consumer 是标准环境、消息有 label。
5. 具体判断：
   - 消息 label 为空：只允许标准 Consumer 消费。
   - 当前 Consumer label 等于消息 label：允许消费。
   - 当前 Consumer 是标准环境，快照新鲜，且 `onlineTrafficLabels` 不包含消息 label：允许标准回退消费。
   - 当前 Consumer 是标准环境，但快照缺失、过期或不可判定：默认返回 `UNKNOWN`，随后非 FIFO `DEFER`、FIFO `BLOCK`。
   - 非 FIFO 下标准 Consumer 遇到仍有在线 owner 的有标消息：返回 `DEFER`。
   - FIFO 下任意 Consumer 遇到不属于自己的队头消息：返回 `BLOCK`。
   - 其他情况：当前 Consumer 不允许消费。
6. 功能默认关闭。`enableTrafficLabelRouting=false` 时保持现有行为完全不变。
7. 为每个路由分支增加单测。

### Task 4：持久化非 FIFO deferred 记录

**文件：**
- 新增：`broker/src/main/java/org/apache/rocketmq/broker/routing/TrafficLabelDeferredRecord.java`
- 新增：`broker/src/main/java/org/apache/rocketmq/broker/routing/TrafficLabelDeferredStore.java`
- 新增：`broker/src/main/java/org/apache/rocketmq/broker/routing/TrafficLabelDeferredIndex.java`
- 新增：基于 commitlog-backed 内部主题的 deferred record 源数据实现，RocksDB 仅作为查询索引/cache。

**记录字段：**
- `recordKey`
- `group`
- `topic`
- `brokerName`
- `queueId`
- `queueOffset`
- `commitLogOffset`
- `messageSize`
- `messageTrafficLabel`
- `storeTimestamp`
- `state`

**步骤：**
1. 以 `brokerName/group/topic/queueId/queueOffset` 作为记录 key。
2. 创建 Broker 内部主题，例如 `RMQ_SYS_TRAFFIC_LABEL_DEFERRED_TOPIC`，用于保存 deferred record 源数据。
3. 在普通 POP cursor 越过不合格消息前，先把 deferred record 幂等写入内部主题；写成功后再更新本地 index/cache。
4. 本地 index 维护：
   - `byLabel: brokerName/group/topic/messageTrafficLabel/queueId/queueOffset -> recordKey`
   - `byQueue: brokerName/group/topic/queueId/queueOffset -> recordKey`
5. 正常扫描 consume queue 前，先查询当前 Consumer 可消费的 deferred record：
   - 隔离 Consumer 走 `byLabel` 前缀，只获取相同 label 的 deferred record。
   - 标准 Consumer 走 `byQueue` 前缀，只返回当前快照新鲜且 `messageTrafficLabel` 不在 `onlineTrafficLabels` 的 deferred record。
6. 标准 Consumer 查询最多扫描 `trafficLabelDeferredMaxScanPerPop` 条 deferred record，不为多个隔离 label 做全量扫描。
7. deferred 消息进入现有 POP in-flight/checkpoint 流程后，才能写删除 tombstone 或删除记录；删除后由 POP invisible/revive/retry 机制负责失败恢复。
8. Broker 重启或主从切换后，从内部主题重建 deferred index，再对外提供 POP 服务。
9. 每次查询 deferred record 时都使用本次 POP 请求的最新在线 Consumer label 快照，不缓存旧的 owner 判断。
10. 增加测试覆盖幂等 upsert、重启恢复、主从切换恢复、删除、隔离 Consumer 上下线后的 fallback eligibility 变化、多 label 查询性能边界。

### Task 5：集成 POP 路由

**文件：**
- 修改：`broker/src/main/java/org/apache/rocketmq/broker/processor/PopMessageProcessor.java`
- 修改：`broker/src/main/java/org/apache/rocketmq/broker/pop/PopConsumerService.java`
- 修改：`remoting/src/main/java/org/apache/rocketmq/remoting/protocol/header/PopMessageRequestHeader.java`
- 修改：`remoting/src/main/java/org/apache/rocketmq/remoting/protocol/header/PopLiteMessageRequestHeader.java`

**步骤：**
1. 在 POP request header 增加 `trafficLabel`、`onlineTrafficLabels`、`trafficLabelSnapshotTimestamp`、`trafficLabelSnapshotVersion`。
2. 非 FIFO POP：
   - 优先投递当前 Consumer 可消费的 deferred record。
   - 正常扫描 consume queue 时，在 subscription filter 通过后执行流量标路由判断。
   - 可消费消息继续走现有 POP result 流程。
   - 不可消费消息先持久化为 deferred record，然后继续扫描，最多扫描 `trafficLabelRoutingMaxScanPerPop` 条。
   - 如果当前是标准 Consumer 且在线 label 快照缺失或过期，遇到有标消息时按不可消费处理并 deferred，不允许 fallback。
3. FIFO POP：
   - 不跳过队头消息。
   - 不为队头消息创建 deferred record。
   - 如果队头消息不属于当前 Consumer，当前 queue 返回空结果。
   - 如果当前是标准 Consumer 且在线 label 快照缺失或过期，队头有标消息时返回空结果，不允许 fallback。
4. 消息一旦被路由投递，后续 POP checkpoint、不可见时间、ack、retry、DLQ 仍由现有流程负责。
5. POP revive/retry 后重新投递时必须再次经过 `TrafficLabelRouteManager`，不能复用上一次投递的 Consumer label。
6. 增加测试覆盖标准消费、匹配隔离消费、标准回退、非 FIFO 越过、FIFO 阻塞、invisible timeout 后重新按最新 label 路由。

### Task 6：补齐延迟和事务消息属性保留

**文件：**
- 修改或确认：`broker/src/main/java/org/apache/rocketmq/broker/schedule/ScheduleMessageService.java`
- 修改或确认：`broker/src/main/java/org/apache/rocketmq/broker/util/HookUtils.java`
- 修改或确认：`broker/src/main/java/org/apache/rocketmq/broker/processor/EndTransactionProcessor.java`
- 修改或确认：`broker/src/main/java/org/apache/rocketmq/broker/transaction/queue/TransactionalMessageUtil.java`
- 修改或确认：`broker/src/main/java/org/apache/rocketmq/broker/transaction/queue/TransactionalMessageBridge.java`
- 测试：`broker/src/test/java/org/apache/rocketmq/broker/schedule/ScheduleMessageServiceTest.java`
- 测试：`broker/src/test/java/org/apache/rocketmq/broker/processor/EndTransactionProcessorTest.java`
- 测试：`broker/src/test/java/org/apache/rocketmq/broker/transaction/queue/TransactionalMessageUtilTest.java`

**步骤：**
1. 写延迟消息属性保留失败测试：构造带 `__RMQ_TRAFFIC_LABEL=gray1` 的延迟消息，到期恢复到真实 Topic 后仍保留该属性。
2. 如果测试失败，修复 `ScheduleMessageService` 或 schedule restore 相关逻辑，只清理延迟系统属性，不清理 `__RMQ_TRAFFIC_LABEL`。
3. 写 timer 延迟属性保留测试：带 timer 属性和 `__RMQ_TRAFFIC_LABEL` 的消息恢复到真实 Topic 后仍保留业务 label。
4. 写事务半消息 commit 属性保留失败测试：half message 带 `__RMQ_TRAFFIC_LABEL=gray1`，commit 后最终业务消息仍保留该属性。
5. 如果测试失败，修复 `TransactionalMessageUtil`、`TransactionalMessageBridge` 或 `EndTransactionProcessor` 的消息属性复制/清理逻辑。
6. 写 rollback 测试：rollback 不产生业务消息，也不产生 deferred record。
7. 确认 schedule topic、transaction half topic、transaction op topic 不接入 `TrafficLabelRouteManager`。

### Task 7：增加指标和日志

**文件：**
- 修改：Broker 侧 POP 路由附近已有 metrics/logging 接入点。

**步骤：**
1. 增加计数指标：
   - 路由到匹配隔离环境。
   - 路由到标准环境回退。
   - 因其他 label 在线而 deferred。
   - FIFO 因其他 label 阻塞。
2. 如果现有 metrics 风格允许，指标 label 包含 `topic`、`group` 和清洗后的 `trafficLabel`。
3. deferred store 写入失败和扫描达到上限时输出限频日志。
4. 标准 Consumer 因 snapshot 缺失/过期而不能 fallback 时输出限频日志，包含 `group`、`topic`、`messageTrafficLabel`、`snapshotAgeMs`、`snapshotVersion`。

## Proxy 实现

### Task 8：解析客户端流量标

**文件：**
- 修改：`proxy/src/main/java/org/apache/rocketmq/proxy/common/ProxyContext.java`
- 修改：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/pipeline/ContextInitPipeline.java`

**步骤：**
1. 在 `ProxyContext` 增加 `trafficLabel`。
2. 在 `ContextInitPipeline` 从 metadata 读取 `GrpcConstants.TRAFFIC_LABEL`。
3. blank label 归一化为空字符串。
4. 增加 Proxy 单测覆盖 metadata 解析。

### Task 9：通过 Proxy 注册 Consumer label

**文件：**
- 修改：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/client/ClientActivity.java`
- 修改：`proxy/src/main/java/org/apache/rocketmq/proxy/processor/ClientProcessor.java`
- 修改：`proxy/src/main/java/org/apache/rocketmq/proxy/service/client/ClusterConsumerManager.java`

**步骤：**
1. 构造 `ClientChannelInfo` 时传入 `ctx.getTrafficLabel()`。
2. 本地注册成功后，把 `trafficLabel` 写入 Proxy 本地 presence manager。
3. unregister 仍使用同一个 channel identity，并确保 Proxy 本地 presence manager 清理对应 label lease。
4. 增加测试覆盖 local proxy 和 cluster proxy 注册路径。

### Task 10：同步多 Proxy 在线 label 状态

**文件：**
- 新增：`proxy/src/main/java/org/apache/rocketmq/proxy/service/client/TrafficLabelPresenceManager.java`
- 修改：`proxy/src/main/java/org/apache/rocketmq/proxy/service/sysmessage/HeartbeatSyncerData.java`
- 修改：`proxy/src/main/java/org/apache/rocketmq/proxy/service/sysmessage/HeartbeatSyncer.java`
- 修改：`proxy/src/main/java/org/apache/rocketmq/proxy/service/client/ClusterConsumerManager.java`
- 测试：`proxy/src/test/java/org/apache/rocketmq/proxy/service/sysmessage/HeartbeatSyncerTest.java`

**步骤：**
1. 增加 `TrafficLabelPresenceManager`：
   - key：`group/topic/trafficLabel/proxyId/clientId/channelId`
   - value：`lastUpdateTimestamp`、`subscriptionDataSet`、`sourceProxyId`
   - 查询：`getOnlineLabels(group, topic)` 返回非空 label set、snapshot timestamp、snapshot version。
2. 在 `HeartbeatSyncerData` 增加 `trafficLabel` 字段。
3. `HeartbeatSyncer#onConsumerRegister` 发送 REGISTER 系统消息时携带 `trafficLabel` 和订阅数据。
4. `HeartbeatSyncer#onConsumerUnRegister` 发送 UNREGISTER 系统消息时携带 `trafficLabel` 或足够的 channel identity，用于远端清理 lease。
5. `HeartbeatSyncer#consumeMessage` 处理远端 REGISTER 时更新 presence manager，处理远端 UNREGISTER 时删除对应 lease。
6. 增加定时清理任务，超过 `trafficLabelPresenceTtlMs` 的 lease 自动过期。
7. 增加单测：
   - Proxy A 注册 `gray1`，Proxy B 收到 sync 后 `getOnlineLabels(group, topic)` 包含 `gray1`。
   - Proxy A unregister 后，Proxy B 清理 `gray1`。
   - Proxy A 崩溃未 unregister 时，Proxy B 在 TTL 后清理 `gray1`。
   - 多个 Proxy、多个 clientId 同 label 时，只要还有一个 lease，label 仍在线。

### Task 11：Receive 时透传 label 和在线快照到 Broker

**文件：**
- 修改：`proxy/src/main/java/org/apache/rocketmq/proxy/processor/ConsumerProcessor.java`
- 修改：`proxy/src/main/java/org/apache/rocketmq/proxy/service/message/LocalMessageService.java`
- 如果 cluster message service 单独构造 POP header，也同步修改对应文件。

**步骤：**
1. 在 `PopMessageRequestHeader` 设置 `trafficLabel`。
2. 在 `PopLiteMessageRequestHeader` 设置 `trafficLabel`。
3. 根据当前 `consumerGroup/topic` 从 `TrafficLabelPresenceManager` 查询 `onlineTrafficLabels`、`snapshotTimestamp`、`snapshotVersion`。
4. 在 `PopMessageRequestHeader` 和 `PopLiteMessageRequestHeader` 设置在线 label 快照。
5. 如果 presence manager 不可用或快照过期，仍发送请求，但 Broker 会按 fail closed 处理有标消息。
6. 不改变 Producer send 的 broker 选择逻辑。
7. 增加测试确认 `ReceiveMessageActivity` 和 lite receive 路径都会透传当前 Consumer label 和在线 label 快照。

## Java Client 实现

### Task 12：增加 Producer 消息 API

**文件：**
- 修改：`/Users/lossend/opensource/rocketmq-clients/java/client-apis/src/main/java/org/apache/rocketmq/client/apis/message/MessageBuilder.java`
- 修改：`/Users/lossend/opensource/rocketmq-clients/java/client/src/main/java/org/apache/rocketmq/client/java/message/MessageBuilderImpl.java`
- 修改：相关 message builder 测试。

**步骤：**
1. 增加 `MessageBuilder setTrafficLabel(String trafficLabel)`。
2. 实现层按普通 property 规则校验非空值。
3. `null` 或 blank 表示忽略或移除该 label。
4. 底层存储为 `__RMQ_TRAFFIC_LABEL`。
5. 现有 `addProperty("__RMQ_TRAFFIC_LABEL", value)` 继续有效。

### Task 13：增加 Consumer 客户端配置 API

**文件：**
- 修改：`/Users/lossend/opensource/rocketmq-clients/java/client-apis/src/main/java/org/apache/rocketmq/client/apis/ClientConfiguration.java`
- 修改：`/Users/lossend/opensource/rocketmq-clients/java/client-apis/src/main/java/org/apache/rocketmq/client/apis/ClientConfigurationBuilder.java`
- 修改：`/Users/lossend/opensource/rocketmq-clients/java/client/src/main/java/org/apache/rocketmq/client/java/rpc/Signature.java`
- 修改：相关 client configuration 和 signature 测试。

**步骤：**
1. 在 `ClientConfiguration` 增加 `trafficLabel` 字段。
2. 在 builder 增加 `setTrafficLabel(String)`。
3. 增加 getter。
4. `Signature.sign()` 仅在 label 非 blank 时写入 `x-mq-traffic-label`。
5. Producer client 也可以携带该配置 label，但 Producer 流量路由只看消息属性 `__RMQ_TRAFFIC_LABEL`。

## 集成测试

### Task 14：Broker POP 路由测试

**场景：**
1. 标准 Consumer 消费无 label 消息。
2. `gray1` 隔离 Consumer 在线时消费 `gray1` 消息。
3. `gray1` 隔离 Consumer 在线时，标准 Consumer 不消费 `gray1` 消息。
4. `gray1` 隔离 Consumer 注销后，标准 Consumer 可以消费旧的 deferred `gray1` 消息。
5. 非 FIFO 下，标准 Consumer 可以越过 deferred 的 `gray1` 消息，消费后续无 label 消息。
6. FIFO 下，队头为 `gray1` 且 `gray1` Consumer 在线时，标准 Consumer 被阻塞。
7. deferred record 在 Broker 重启后仍能恢复。
8. `gray1` 隔离 Consumer 已拿到消息但未 ack 时，标准 Consumer 在 invisible time 到期前不能消费该消息。
9. invisible time 到期后，如果 `gray1` 隔离 Consumer 已离线，标准 Consumer 可以 fallback 消费该重试消息。
10. invisible time 到期前或重试投递前 `gray1` 隔离 Consumer 重新上线时，该消息继续优先投递给 `gray1` 隔离 Consumer。
11. retry/revive 消息保留 `__RMQ_TRAFFIC_LABEL`，每次重新投递都重新执行路由判定。
12. 延迟消息到期恢复真实 Topic 后保留 `__RMQ_TRAFFIC_LABEL`，并按普通消息路由。
13. FIFO 延迟消息到期恢复真实 Topic 后保留 `__RMQ_TRAFFIC_LABEL`，并按 FIFO 队头规则路由。
14. 事务半消息 commit 后最终业务消息保留 `__RMQ_TRAFFIC_LABEL`，并按普通或 FIFO 规则路由。
15. 事务 rollback 不产生业务可消费消息，也不产生 deferred record。
16. 标准 Consumer 请求缺少 `onlineTrafficLabels` 或 snapshot 过期时，非 FIFO 有标消息进入 deferred，不允许 fallback。
17. 标准 Consumer 请求缺少 `onlineTrafficLabels` 或 snapshot 过期时，FIFO 有标队头返回空结果，不允许 fallback。
18. deferred record 源数据从 commitlog-backed 内部主题重建后，Broker 重启仍能重新投递被跳过消息。

### Task 15：Proxy + Java Client 集成测试

**场景：**
1. Java client Producer 调用 `setTrafficLabel("gray1")` 后，Broker 存储消息属性 `__RMQ_TRAFFIC_LABEL`。
2. Java client Consumer 调用 `setTrafficLabel("gray1")` 后，经 Proxy 注册并消费匹配消息。
3. 标准 Java client 不发送 `x-mq-traffic-label`，可以消费 fallback 消息。
4. lite push/simple receive 路径都会透传 label。

### Task 16：多 Proxy 多 Broker 集成测试

**场景：**
1. 两个 Proxy、两个 Broker master，topic queue 分布在两个 master 上。
2. `gray1` Consumer 连接 Proxy A，标准 Consumer 连接 Proxy B。
3. Proxy B 的 presence manager 通过 heartbeat sync 得知 `gray1` 在线。
4. 标准 Consumer 通过 Proxy B 从 Broker master 1 拉取时，不能消费 `gray1` 消息。
5. 标准 Consumer 通过 Proxy B 从 Broker master 2 拉取时，也不能消费 `gray1` 消息。
6. `gray1` Consumer unregister 后，Proxy B presence manager 清理 `gray1`，标准 Consumer 可以从两个 master fallback 消费旧的 deferred `gray1` 消息。
7. Proxy A 崩溃但未发送 unregister 时，Proxy B 在 TTL 前仍认为 `gray1` 在线；TTL 后才允许标准 fallback。
8. Broker master 1 重启后，从内部 deferred topic 重建 index，之前被标准 Consumer 跳过的 `gray1` 消息仍可被 `gray1` 或标准 fallback 消费。
9. Broker master 1 切换或恢复期间，如果 deferred index 未准备好，该 Broker 不对外提供 traffic-label POP，避免 offset 已推进但 deferred 不可见。

## 验收标准

- 功能默认关闭，`enableTrafficLabelRouting=false` 时现有行为零变化。
- 功能开启后，非 FIFO 标准 Consumer 跳过有标消息时不能丢消息。
- 匹配隔离 Consumer 下线后，旧的有标消息可以回退给标准 Consumer。
- 匹配隔离 Consumer 已经拿到消息但未 ack 时，标准 Consumer 只能在 POP invisible time 到期且 Proxy/Broker 确认该 label 无在线 Consumer 后重新消费。
- retry/revive 消息每次投递都重新按最新在线 Consumer label 计算路由。
- FIFO 模式保持全局队列顺序。
- 延迟/定时消息在内部延迟路径不做业务路由，到期恢复真实 Topic 后保留 `__RMQ_TRAFFIC_LABEL` 并参与动态路由。
- 事务半消息和 op 队列不做业务路由，commit 后最终业务消息保留 `__RMQ_TRAFFIC_LABEL` 并参与动态路由。
- 多 Proxy 下，一个 Proxy 上存在 `gray1` Consumer 时，其他 Proxy 上的标准 Consumer 也不能 fallback 消费 `gray1` 消息。
- 多 Broker master 下，每个 master 都基于同一份 Proxy 集群在线 label 快照做本地 queue 路由，不因 Broker 本地看不到某个 Consumer 就提前 fallback。
- 在线 label 快照缺失、过期或不可判定时默认 fail closed：非 FIFO deferred，FIFO block，不允许标准 Consumer fallback。
- deferred record 的源数据位于 commitlog-backed 内部主题或等价 HA 持久化域；Broker 重启或 master 切换后能重建 deferred index。
- 现有 POP ack、change invisible、retry、DLQ 测试继续通过。
- Java 5.x API 清晰区分 Producer 消息流量标和 Consumer 环境流量标。

## 建议验证命令

先跑 RocketMQ 侧重点模块：

```bash
mvn -pl common,remoting,broker,proxy -DskipITs -DskipCheckstyle test
```

再到 Java client 仓库跑 client API 和实现测试：

```bash
cd /Users/lossend/opensource/rocketmq-clients/java
mvn -pl client-apis,client -DskipITs test
```

最后回到 RocketMQ 仓库跑相关模块完整检查：

```bash
cd /Users/lossend/opensource/rocketmq
mvn -pl common,remoting,broker,proxy -DskipITs test
```

## 实现注意事项

- 不要只在 Proxy 实现该能力。Broker 才是正确性边界，因为 offset 推进、POP checkpoint、不可见时间、重试、DLQ 和 FIFO 顺序都在 Broker 侧。
- 不要把该能力实现成普通 SQL/tag filter。普通过滤无法安全表达“根据在线隔离 Consumer 动态回退”的语义。
- 非 FIFO 下，不允许在 deferred record 持久化成功前推进原始 POP cursor 越过不合格消息。
- 旧 remoting 客户端未携带 consumer `trafficLabel` 时，必须按标准环境处理。
