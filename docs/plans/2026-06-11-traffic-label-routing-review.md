# 流量标动态路由设计 Review

> 针对 `docs/plans/2026-06-11-traffic-label-routing.md` 的设计评审
> 评审日期：2026-06-26

---

## 总体评价

| 维度 | 等级 | 关键风险 |
|------|------|---------|
| 逻辑正确性 | 良好 | YIELD 忙轮询、FIFO 无超时兜底、snapshot 窗口隔离泄漏 |
| 性能 | 良好 | DEFER 写放大已有缓解；建议增加 fast path 和 header 压缩 |
| 高可用 | 中等 | Proxy 崩溃 TTL 窗口、index 重建期间不可用、网络分区双重投递 |
| 数据安全 | 中等 | commitlog 过期致消息不可达、feature 关闭后残留、compaction 未定义 |

整体设计质量很高，核心路由模型正确，非对称 DEFER/YIELD 规则是关键创新点。主要改进建议集中在**边界场景的兜底机制**（超时、降级、清理）和**运维可观测性**。

---

## 一、逻辑正确性

### 1.1 YIELD_WITHOUT_DEFER 可能导致忙轮询

**问题描述：**

隔离 Consumer 遇到标准消息且 `STANDARD` 在线时，返回 `YIELD_WITHOUT_DEFER`——不推进 cursor、不写 deferred、返回空结果。但下一次 POP 该 Consumer 仍然会扫到同一条消息，再次 YIELD，形成热循环。

文档未描述退避/挂起机制。

**建议：**

- 对连续 YIELD 的 queue 做短时退避（类似 POP long polling suspend）
- 或记录连续 YIELD 次数，超过阈值后暂时跳过该 queue
- 增加指标 `traffic_label_route_yield_consecutive_count{topic,group,queueId}` 用于监控

---

### 1.2 FIFO 场景可能无限阻塞

**问题描述：**

如果 `gray1` Consumer 在线但消费极慢（或假死但 lease 未过期），FIFO 队列对标准 Consumer 是无限期 BLOCK。文档承认了"顺序优先于吞吐"，但没有兜底超时。

**风险场景：**

- `gray1` Consumer 进程 GC 卡顿或死循环，但 TCP 连接存活
- Proxy lease 未过期，Broker 一直认为 `gray1` 在线
- FIFO queue 对所有其他 Consumer 完全阻塞

**建议：**

- 增加可配置的 `trafficLabelFifoBlockMaxMs`，超时后允许标准 Consumer fallback（需配合告警）
- 或在 Broker 侧增加"FIFO queue stall detection"，当同一 queue head 连续 N 秒无法投递时告警

---

### 1.3 Snapshot 新鲜度窗口内的隔离泄漏

**问题描述：**

场景：snapshot 生成时 `gray1` 尚未注册，但在 Broker 处理 POP 之前 `gray1` 上线。此时 snapshot 说 `gray1` offline，标准 Consumer 会 fallback 消费 `gray1` 消息——这是一个短暂的隔离违反窗口。

默认 `trafficLabelRoutingMaxSnapshotAgeMs=30000`（30 秒）已经很保守，但仍有瞬时违反可能。

**建议：**

- 文档中明确承认这是 AP 系统的固有 trade-off，而非 bug
- 对隔离严格场景建议缩短 snapshot age（如 5-10s），代价是更频繁的 DEFER/BLOCK
- 在验收标准中注明：隔离保证是最终一致的，不是强一致的

---

### 1.4 隔离 Consumer 并发扫描同一位置的正确性

**问题描述：**

当 `gray1` Consumer 扫到 `gray2` 消息（`gray2` 确认离线）时会写 DEFER。如果 N 个隔离 Consumer 同时扫到同一条离线 label 消息，第一个写 ACTIVE 后其余幂等返回——幂等保证是关键，文档已覆盖。

但需确认：多个并发 POP 中只有一个能成功推进 cursor（因为 POP cursor 本身有锁），否则可能重复处理同一位置。

**建议：**

- 明确 POP cursor 推进与 deferred ACTIVE 写入之间的并发控制模型
- 说明是否依赖现有 POP 的 queue lock 机制

---

## 二、性能

### 2.1 每次 DEFER 的写放大

**问题描述：**

一次 DEFER = 1 次 internal topic 写入 + 1 个 RocksDB WriteBatch（4 个 key 更新）。高流量场景下（如标准 Consumer 遇到大量在线隔离 label 消息），写入压力可能显著。

文档通过 `YIELD_WITHOUT_DEFER` 避免隔离 Consumer 把标准消息写入 ledger，这是关键优化。但标准 Consumer 仍然会为每条在线隔离消息写 DEFER。

**建议：**

- 量化典型场景压力：100k msg/s、10% 隔离消息 → ~10k deferred writes/s per Broker
- 评估是否需要批量 WriteBatch（将同一次 POP scan 中的多条 DEFER 合并为一个 batch）
- 或考虑异步写 internal topic + 同步写 RocksDB index 的混合模式

---

### 2.2 POP 请求延迟增加

**问题描述：**

功能开启后，每次 POP 增加：
1. 解析 `onlineTrafficLabels` 为 HashSet
2. 查询 RocksDB deferred index
3. 每条消息的路由判断

对于没有 deferred backlog 的正常场景，步骤 2 应该很快返回空（prefix scan 命中空前缀）。

**建议：**

- 增加 **fast path**：如果 `activeLabelCache` 为空（无任何 deferred），直接跳过 deferred 查询
- 如果所有消息都是标准消息且 Consumer 也是标准，路由判断应该是 O(1) short-circuit
- 增加 POP latency percentile 指标区分 traffic-label-enabled vs disabled

---

### 2.3 onlineTrafficLabels 头部膨胀

**问题描述：**

每次 POP 请求都携带完整在线 label 列表（逗号分隔）。如果有 50+ 隔离环境，header 可能达到几 KB。

**建议：**

- 设置 label 数量上限（如 128）或 header size 上限（如 4KB）
- 或改为版本号 + Broker 侧缓存机制：Proxy 携带 snapshot version，Broker 对比版本，版本相同则使用缓存，不同时才解析新 label 列表
- 或使用 bitmap/bloom filter 压缩在线状态

---

### 2.4 Internal Topic 重放时间

**问题描述：**

deferred 事件全部写入同一个 internal topic。Broker 恢复时需要全量重放重建 RocksDB index。如果积累了数百万条 ACTIVE/TOMBSTONE 事件对，重放时间可能以分钟计。

**建议：**

- 定期做 checkpoint（记录已重放到的 offset + RocksDB snapshot），重启只重放增量
- 或定期做 RocksDB full snapshot 持久化到独立文件
- 文档中给出重放性能基准：预期 X 万条事件的重放时间上限

---

## 三、高可用

### 3.1 Proxy 崩溃后 TTL 过期前的可用性损失

**问题描述：**

Proxy 崩溃但未发送 unregister，其上 Consumer 的 label 在 TTL 前仍被认为在线。TTL 期间：
- 标准 Consumer 无法 fallback 消费这些 label 的消息
- 消息要么 DEFER（非 FIFO）要么 BLOCK（FIFO）

**文档未给出 `trafficLabelPresenceTtlMs` 默认值。**

**建议：**

- 推荐默认值（如 30s-60s），并解释选择依据
- TTL 太短 → 正常网络抖动导致 label flapping → 隔离泄漏
- TTL 太长 → Proxy 真实宕机时影响可用性
- 增加指标 `traffic_label_presence_lease_expired_total{proxyId,label}` 用于区分主动注销和 TTL 过期

---

### 3.2 HeartbeatSyncer 系统 Topic 不可用

**问题描述：**

如果承载 heartbeat sync 的系统 topic 所在 Broker 不可用：
- 新 Proxy 无法学习其他 Proxy 上的 Consumer 状态
- 已有 presence 会逐渐 TTL 过期
- 最终所有 Proxy 只看到自己本地 Consumer

文档未描述此降级场景。

**建议：**

- 补充 syncer 不可用时的行为说明
- 是否降级为 fail closed（有标消息全部 DEFER/BLOCK）？
- 是否需要告警"presence sync stale > threshold"？
- 是否需要备用 sync 通道（如 Proxy 间直连 gossip）？

---

### 3.3 Broker Index 重建期间服务不可用

**问题描述：**

文档明确写了"Broker master 切换或恢复期间...不对外提供 traffic-label POP"。但这意味着该 Broker 上所有需要流量标路由的 queue 暂时不可用。

**建议：**

- 预估重建时间的 SLA（如 100 万条事件重放需要多少秒？）
- 是否可以提供降级选项：先按"不做流量标路由、全部按标准模式消费"来保可用性，再异步重建 index
- 增加配置 `trafficLabelDeferredIndexRebuildTimeoutMs`，超时后降级
- Broker 重建进度需要暴露到 metrics 和 admin 接口

---

### 3.4 网络分区下的双重投递

**问题描述：**

如果 Proxy A 和 Proxy B 网络隔离（但各自能连 Broker）：
- Proxy A 认为 `gray1` 在线（连在自己上面）
- Proxy B 因 sync 中断，TTL 后认为 `gray1` 离线
- 标准 Consumer 通过 Proxy B 的 snapshot 告诉 Broker `gray1` 离线
- `gray1` Consumer 通过 Proxy A 的 snapshot 告诉 Broker `gray1` 在线

此时同一条 `gray1` 消息可能被 `gray1` 和标准 Consumer 同时消费（从不同 Broker master 的不同 queue）。

**建议：**

- 文档明确声明：在网络分区场景下，隔离保证降级为 at-least-once + 短时重复消费
- 不保证严格隔离（CAP 中选择 A+P）
- 业务方对隔离敏感的场景需要在消费端做幂等

---

## 四、数据安全

### 4.1 Deferred Record 超过 Commitlog 保留时间

**问题描述：**

如果隔离 Consumer 长期离线，deferred record 可能一直保持 ACTIVE。当原消息 commitlog 被清理后，record 指向的物理位置不可读——消息事实上丢失。

文档说"超过后只告警和限流，不静默 fallback 或删除"。但没有回答：**当消息确实不可读时怎么办？**

**建议：**

- 当 `readMessage` 返回 null（commitlog 已清理）时：
  1. 写 TOMBSTONE 清理该 record
  2. 将该消息的关键信息（recordKey、label、offset、原始 storeTimestamp）写入 error/DLQ topic
  3. 输出指标 `traffic_label_deferred_message_expired_total{topic,group,label}`
- 增加运维建议：`trafficLabelDeferredMaxHoldMs` 应设置为 `fileReservedTime * 0.8`，留安全余量

---

### 4.2 Feature 运行时关闭后的 Deferred 残留

**问题描述：**

如果将 `enableTrafficLabelRouting` 从 true 改为 false：
- 已有 deferred records 不会被消费（因为路由逻辑关闭了，deferred 查询不再触发）
- 对应消息的 cursor 已经推进过了
- 这些消息就永远停留在 ledger 中，事实上丢失

**建议：**

- 补充 graceful disable 流程：
  1. 先标记为 `draining` 模式：不再写新 ACTIVE，但继续投递已有 deferred（全部按标准 Consumer fallback）
  2. drain 完成后再关闭 feature
- 或至少在关闭时告警："检测到 N 条未消费 deferred records，请先 drain"
- admin 命令支持手动触发 drain

---

### 4.3 Label 长度和数量没有硬限制

**问题描述：**

文档限制字符集为 `[A-Za-z0-9._-]`，但未限制：
- 单个 label 最大长度
- 单个 consumer group 最大在线 label 数量

极端情况下可能导致：
- RocksDB key 过长，影响查询性能
- `onlineTrafficLabels` header 过大，触发 Netty frame limit
- `activeLabelCache` 内存膨胀

**建议：**

- 单个 label 最大长度：64 字符
- 单个 consumer group 最大在线 label 数：128
- 超过限制时拒绝注册并返回明确错误
- Broker 侧对 `onlineTrafficLabels` header 做长度校验

---

### 4.4 Internal Topic Compaction 策略未定义

**问题描述：**

文档提到"后台根据 TOMBSTONE 和 replay checkpoint 做压缩或过期清理"，但没有具体策略。如果不做 compaction：
- Internal topic 无限增长
- 重启重放时间线性增长
- 磁盘空间持续消耗

**建议：**

- 定义 compaction 策略：
  1. 维护 replay checkpoint offset（所有 Broker 已确认重放到的最小 offset）
  2. checkpoint offset 之前的消息可以被清理
  3. 同 key 的 ACTIVE + TOMBSTONE pair，在 TOMBSTONE 被所有副本确认且 replay checkpoint 过后，可安全删除
- 或使用 topic TTL（如 `3 * fileReservedTime`）+ 定期 full RocksDB checkpoint
- 增加指标 `traffic_label_deferred_internal_topic_lag{broker}` 监控 topic 积压

---

### 4.5 claimedKeys 内存态的并发安全

**问题描述：**

`claimedKeys` 是内存态 ConcurrentHashSet（或类似结构），用于避免同 Broker 内并发 POP 重复投递同一 deferred record。

需确认：
- claim 的生命周期管理：claim 后如果线程异常退出，是否有超时自动释放？
- claim 的内存上限：如果大量 deferred records 被 claim 但长时间未完成（如 tombstone 重试），是否会导致内存膨胀？

**建议：**

- claim 增加超时机制（如 60s），超时后自动释放
- 增加 `traffic_label_deferred_claimed_keys_count{broker}` 指标
- 限制同时 claimed 的 key 数量上限

---

## 五、补充建议

### 5.1 可观测性增强

建议增加以下运维工具：

- Admin 命令 `queryDeferredBacklog(topic, group)`：返回各 label 的 active count、oldest age
- Admin 命令 `drainDeferred(topic, group, label)`：手动将指定 label 的 deferred records 转为 fallback 可消费
- Admin 命令 `resetDeferredIndex(broker)`：强制从 internal topic 重建 index

### 5.2 灰度发布策略

建议补充灰度上线方案：

1. 先对单个 consumer group 开启（group 级别开关）
2. 验证该 group 的路由、deferred、fallback 正常
3. 再逐步扩大到全 Broker

### 5.3 压力测试场景

建议补充以下压力测试用例：

- 100 个隔离 label + 1 个标准 Consumer，100k msg/s，80% 标准消息
- 隔离 Consumer 频繁上下线（每 10s 切换一次），验证 deferred 和 fallback 抖动
- Broker 在大量 deferred backlog 下重启，测量 index 重建时间
- POP 并发 100 线程，验证 claimedKeys 无死锁

---

## 六、结论

设计文档覆盖了核心场景和关键细节，特别是：
- 非对称 DEFER/YIELD 规则有效防止标准流量放大
- Commitlog-backed internal topic + RocksDB index 的分层存储设计合理
- Fail closed 策略正确保护隔离语义

需要重点补充的是：
1. **边界场景的兜底超时**（YIELD 退避、FIFO 超时、claim 超时）
2. **运维生命周期管理**（compaction 策略、feature disable drain、index 重建 SLA）
3. **硬限制和降级**（label 长度/数量限制、header size 限制、分区降级声明）
