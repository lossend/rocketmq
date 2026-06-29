---
name: traffic-label-routing-plan-b-subcursor-harvest
description: 方案 B —— per-label 子游标 + 标准收割:不复制消息,标准消费者收割离线隔离标的子游标
date: 2026-06-29
status: brainstorming
---

# 方案 B:per-label 子游标 + 标准收割

> 顶层设计见 [[2026-06-29-traffic-label-routing-design]]。
> 对照方案见 [[2026-06-29-traffic-label-routing-plan-a-strict-bypass]]。

## 1. 先破一个错觉

"每个环境一个独立游标 + SKIP" 单独并 **兜不了底**。反例:

```
queue: [stdA, gray1B, stdC]
G@STD 与 G@gray1 各扫各的

t0 gray1 在线: G@STD 扫到 gray1B → SKIP,游标永久越过 B 去取 stdC
t1 gray1 崩溃,且 G@gray1 还没消费到 B
结果: B 卡死 —— G@STD 不会回头,G@gray1 没了主人。没人能消费 B。
```

SKIP 是单向的:越过即丢给对方。所以方案 B 必须在子游标之上 **再加一层"收割"**。

## 2. 核心机制:标准是离线标的"收割者"

把每个流量标看作 group 在同一条 queue 上的一个 **子游标**:

```
G@STD     -> 标准环境消费进度
G@gray1   -> gray1 环境消费进度
G@gray2   -> gray2 环境消费进度
```

它们都扫 **同一条物理 queue**,各带一个 filter。Broker 处理一次 POP 时,按请求里的 `consumerLabel` + 在线快照决定读哪些子游标:

**gray1 消费者 POP** → 只推进 `G@gray1`:投递 `label==gray1`,其余 SKIP。

**标准消费者 POP** → 分两段读:
1. **主段**,推进 `G@STD`:
   - `label==STANDARD` → 投递。
   - `label==grayX 且 grayX 在线` → SKIP(留给 grayX 自己的子游标)。
   - `label==grayX 且 grayX 离线` → 投给标准(新消息兜底)。
2. **收割段**:对快照里每个 **离线** label,从其子游标(如 `G@gray1`)当前 offset 往后读,filter `label==gray1`,投给标准,并推进 `G@gray1`。

## 3. B 怎么被救回

```
t0 gray1 在线: G@STD 扫过 B(SKIP),但 B 仍由 G@gray1 记账(gray1 未读到)
t1 gray1 离线
t2 标准 POP: 主段无新标准消息; 收割段发现 gray1 离线
            → 从 G@gray1 的 offset 读到 B → 投给标准 → 推进 G@gray1 ✓
            B 被标准消费,没复制、没旁路
```

`G@STD` 当初 SKIP 掉的消息没丢,因为它们一直被 `G@gray1` 记着账;gray1 一离线,标准就把这条子游标抽干。临时环境销毁也天然处理:死掉的 gray 子游标被标准收割到队尾即退休。

## 4. 关键改造点

### 4.1 Proxy
- 维护流量标在线快照(同方案 A)。
- POP 请求携带 `consumerLabel` + 在线快照。

### 4.2 Broker / `PopMessageProcessor`
- **子游标管理**:offset 记账维度从 `(group, queue)` 扩展为 `(group, label, queue)`。
- **两段读逻辑**:标准 POP 时主段 + 收割段;隔离 POP 时单段。
- 复用每条子游标自带的 POP checkpoint / ack / retry,**无 revive 特判**。

### 4.3 并发竞争(主要难点)
- gray1 收割途中突然回线,标准与 gray1 可能同时读 `G@gray1`。
- 缓解:`QueueLockManager` 按 `(group, label, queueId)` 串行 + 在线快照判定。
- 残余窗口是 at-least-once,需消费端幂等。

## 5. 现成轮子候选:LMQ

RocketMQ 有 **LMQ(Light Message Queue)**:按消息属性把消息 dispatch 进每个 LMQ 独立的 consume queue,**自带独立 offset** —— 几乎就是"per-label 子游标"的内建实现。

- 每个流量标 = 一个 LMQ,可能省掉自造子游标。
- 待核实:LMQ 原本面向"一条消息扇出到多个队列",其 dispatch 成本与语义是否契合本场景。
- 符合"先找成熟轮子"原则,出方案时需认真评估。

## 6. 优点

- **不复制消息体**:无 route topic、无旁路存储、无 route index。
- **零 revive 特判**:失败重试复用每条子游标的 POP retry。
- **死环境自然退休**:收割到队尾即停。
- 可能直接复用 LMQ,改动面更小。

## 7. 缺点 / 风险

- **子游标元数据膨胀**:offset 记录数 = label 数 × queue 数。十几个 label 没问题;label 极多时膨胀。
- **收割并发竞争**:gray 回线与标准收割的竞态,需锁 + 幂等兜底。
- **回退实时性中等**:依赖标准收割轮次,非即时。
- **在线快照强依赖**:标准必须准确知道"哪些 label 存在且离线"才能收割,快照错误会误收割或漏收割。

## 8. 简化版(若隔离环境常驻)

若隔离环境 **常驻、仅偶尔抖动**:可退化为 **纯子游标、不收割**(语义③)。抖动时 gray1 回来自己继续读,不串环境,最简单。此时方案 B 几乎零额外机制。

## 9. 适用场景

- 隔离环境 **常驻灰度** → 用简化版(纯子游标)。
- 隔离环境 **临时但 label 数可控** → 用完整收割版。
- label 数量极大时,子游标膨胀使方案 B 退化,此时倾向 [[2026-06-29-traffic-label-routing-plan-a-strict-bypass]]。
