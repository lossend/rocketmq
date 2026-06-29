---
name: traffic-label-routing-design
description: 基于流量标的 RocketMQ 动态消费路由 —— 顶层设计与方案索引
date: 2026-06-29
status: brainstorming
---

# 基于流量标的动态消费路由 —— 顶层设计

> 本文是顶层设计索引。两个候选方案的细节分别见:
> - [[2026-06-29-traffic-label-routing-plan-a-strict-bypass]] —— 方案 A:严格隔离 + 旁路存储
> - [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] —— 方案 B:per-label 子游标 + 标准收割
>
> 历史参考(**本次设计已独立思考,不默认沿用**):
> - [[2026-06-26-traffic-label-routing-server-side-pop-retry]] —— 旧的 route-topic / POP retry 方案

## 1. 背景与需求

部署上存在两类环境:

- **标准环境(Standard)**:常驻,承接全量兜底流量。
- **隔离环境(Isolated / gray)**:可同时存在多个(gray1、gray2…),用于灰度 / PR 预览 / 压测等。

应用可部署在标准环境或某个隔离环境。期望行为:

1. 隔离环境的 **生产者** 给每条消息打上 **流量标(isolation tag)**,载体为消息属性 `__RMQ_TRAFFIC_LABEL`。
2. 若存在 **同流量标的隔离环境消费者在线** → 该消息由对应隔离环境消费。
3. 否则 → 消息 **回退** 给标准环境消费者消费。

约束:**Proxy 负责消费路由**;必须支持 **分布式部署**、**高可用**、**高性能**。

消费模型:**仅 POP / gRPC 5.x**(已确认,无 PULL/PUSH 经典模型,是最干净的情形)。

## 2. 根本张力:POP 单游标

POP 模型下,一个 `(consumerGroup, topic, queueId)` 共享 **一个 offset 游标**。本需求要求在同一条物理 queue 上对 **单条消息粒度** 做"投给谁"的判断,这与单游标天然冲突:

> "顺序单游标" + "选择性跳过部分消息" + "被跳过的消息日后仍能被其他环境消费" —— 三者不能同时廉价满足。

跳过一条消息(SKIP)是 **单向** 的:游标越过即把这条消息丢给对方,自己不会回头。因此要让"被标准跳过的隔离消息日后仍能回退给标准",**单纯 SKIP 做不到**,必须二选一:

- **A. 把消息复制到旁路** —— 越过前先落到另一处存储,隔离消费者从旁路读。
- **B. 不复制,改让标准去"收割"离线隔离标的子游标** —— 每个流量标一个独立子游标,标准在隔离标离线时把它的子游标抽干。

这两条路就是下文的方案 A 与方案 B。

## 3. 架构层结论

### 3.1 能否纯靠扩展点(不改核心)?

**不能完整实现。** 依据 [[Server_Extension_Points]](`docs/cn/Server_Extension_Points.md`):

- Proxy 的 `PopMessageResultFilter.FilterResult` 只有 `{TO_DLQ, NO_MATCH, MATCH, TO_RETURN}` —— 无法改写 header、无法跨 topic 搬运消息、无法触发回退拉取。
- Store 的 `MessageFilter`(`isMatchedByConsumeQueue` / `isMatchedByCommitLog`)在 pop 路径有调用点,可按消息属性决定 match/skip,但 **不持有在线视图、不能做有状态回退路由**。

结论:纯扩展点最多实现 **弱化版**(隔离精确匹配 + 标准盲回退),无法满足"在线才隔离、离线才回退"的动态语义。

### 3.2 必须改 Proxy + Broker

动态回退依赖 **流量标在线快照**(哪些 label 存在、是否在线),这是有状态的集群视图,必须:

- **Proxy 侧**:基于 `ClusterConsumerManager` + `HeartbeatSyncer` 维护跨实例的 label 在线快照,随 POP 请求把 `consumerLabel` + 在线快照下发给 Broker。
- **Broker 侧**:在 `PopMessageProcessor` 收敛单粒度路由决策。

两方案都建立在这个 Proxy+Broker 改造基线上,差异只在 Broker 如何处理"被跳过的隔离消息"。

## 4. 两方案对比

| 维度 | 方案 A:严格隔离 + 旁路存储 | 方案 B:子游标 + 标准收割 |
|---|---|---|
| 核心机制 | 越过前把隔离消息复制到旁路存储,隔离消费者从旁路读 | 每个流量标一个独立子游标;标准在隔离标离线时收割其子游标 |
| 是否复制消息体 | 是 | 否 |
| 是否需要 route topic / 旁路存储 | 是 | 否 |
| offset 记录数 | 标准 + 旁路 | label 数 × queue 数(随 label 增长膨胀) |
| revive / retry | 需对旁路做特判 | 复用每条子游标自带的 POP retry,零特判 |
| 回退实时性 | 高(旁路独立投递) | 中(依赖标准收割轮次) |
| 主要风险 | 复制写放大、旁路存储一致性、revive 特判复杂 | 子游标元数据膨胀、收割并发竞争、label 极多时退化 |
| 复杂度 | 高 | 中 |
| 现成轮子候选 | route topic(旧方案思路) | **LMQ(Light Message Queue)** 内建独立 offset,待评估 |

## 5. 共性问题(两方案都要处理)

- **抖动窗口跨环境泄漏**:隔离标在线/离线判定有快照延迟,窗口内可能误投。
- **at-least-once 重复投递**:收割 / 旁路切换的残余窗口需消费端幂等(RocketMQ 本就要求)。
- **流量标在线快照** 的准确性与同步延迟,是两方案共同的可用性关键。

## 6. 选型未决项

隔离环境的 **生命周期** 直接决定选型:

- **常驻、偶尔抖动**(常驻灰度):语义可退化为"纯子游标、不收割"(方案 B 的简化版),最简单。
- **临时、频繁销毁**(PR 预览 / 压测):死环境的消息会永久卡死,**必须** 有方案 A 的旁路或方案 B 的收割机制。

> ⏳ 待用户确认隔离环境生命周期后,再给出最终推荐与 2-3 方案收敛。
