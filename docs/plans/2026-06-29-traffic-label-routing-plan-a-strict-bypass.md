---
name: traffic-label-routing-plan-a-strict-bypass
description: 方案 A —— 严格隔离 + 旁路存储:越过前复制隔离消息,隔离消费者从旁路读
date: 2026-06-29
status: brainstorming
---

# 方案 A:严格隔离 + 旁路存储

> 顶层设计见 [[2026-06-29-traffic-label-routing-design]]。
> 对照方案见 [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]]。

## 1. 核心思路

POP 单游标越过(SKIP)一条消息是单向的。方案 A 的解法:**在标准游标越过某条隔离消息之前,先把这条消息复制到一处旁路存储**,隔离环境消费者从旁路读取自己的消息;标准游标随后正常前进。这样原物理 queue 的游标永远单调前进,不被任何环境阻塞。

一句话:**用空间(复制一份)换取游标的单调前进**。

## 2. 语义

- 隔离消息 `label==grayX`:
  - 若 grayX **在线** → 复制到 grayX 的旁路 → 由 grayX 从旁路消费;标准游标越过。
  - 若 grayX **离线** → 不复制,直接投给标准(回退)。
- 标准消息 `label==STANDARD` → 直接投给标准。

"在线/离线"以 Proxy 下发的 **流量标在线快照** 为准。

## 3. 数据流

```
producer(gray1) --B(label=gray1)--> 物理 queue
                                       │
                  标准消费者 POP ───────┤ 扫到 B
                                       │  快照判定 gray1 在线
                                       ▼
                              复制 B 到旁路(gray1 专属)
                                       │ 标准游标越过 B
                                       ▼
                  gray1 消费者 POP ──> 旁路(gray1) 读到 B ✓
```

## 4. 关键改造点

### 4.1 Proxy
- 维护流量标在线快照(`ClusterConsumerManager` + `HeartbeatSyncer`)。
- POP 请求携带 `consumerLabel` + 在线快照(或快照版本号,Broker 侧缓存)。

### 4.2 Broker / `PopMessageProcessor`
- 单粒度路由决策:读消息属性 `__RMQ_TRAFFIC_LABEL`,结合快照决定 **投递 / 复制到旁路 / 越过**。
- 复制写入:在 `appendCheckPoint` 之前,对"在线隔离标"的消息执行旁路写入,失败则不推进游标(保证不丢)。

### 4.3 旁路存储形态(待定子项)
两种候选,需评估:
- **route topic**:复用旧方案思路(系统 topic + routeKey=`group/topic/label`)。成熟但重。
- **per-label 独立队列**:每个 label 一条逻辑队列,语义更清晰。

### 4.4 revive / retry 特判
- 旁路里的消息有自己的 checkpoint / ack / changeInvisibleTime。
- `PopReviveService` 需要识别旁路来源,重试落回旁路而非原 queue。**这是方案 A 复杂度的主要来源。**

## 5. 优点

- **回退实时性高**:旁路独立投递,隔离消费者不被标准游标牵制。
- **隔离边界清晰**:每个环境读自己的旁路,物理隔离强。
- **死环境天然不卡主 queue**:主 queue 游标永远前进。

## 6. 缺点 / 风险

- **写放大**:每条在线隔离消息复制一次。
- **旁路一致性**:复制 + 原游标推进必须保证"先落旁路再越过",否则丢消息;跨存储一致性需小心。
- **revive 特判复杂**:重试路径要区分旁路 / 主 queue,改动 `PopReviveService` 较深。
- **旁路存储元数据**:per-label 队列或 route topic 都引入额外 offset / checkpoint 管理。

## 7. 与旧方案的关系

旧的 [[2026-06-26-traffic-label-routing-server-side-pop-retry]] 本质就是方案 A 的一种重实现(route topic + DELIVER/YIELD/MOVE 动作 + revive 特判)。本方案 A 抽取其"复制到旁路"内核,但 **不默认绑定 route topic**,旁路形态留作可评估子项。

## 8. 适用场景

- 隔离环境 **临时、频繁销毁**(PR 预览 / 压测):旁路让死环境消息立即回退标准,主 queue 不受影响。
- 对 **回退延迟敏感** 的业务。
