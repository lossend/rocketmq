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

> 📌 **精确化(2026-06-29 补充)**:对 plan-b 做了 5 能力 × 扩展点逐条核对,发现存在一个 **零 broker 侵入的静态隔离变体 Plan-B-Lite**(预创建真实组 `G%label` + SQL92 属性过滤 + `enablePropertyFilter`),但它 **缺动态回退**。完整对照表与补回退两条路径见 [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] §8-§9。要点:
> - Proxy `PopMessageResultFilter` 的 `NO_MATCH` 会 **ACK 丢消息**,是回退陷阱,不可用(`ConsumerProcessor.java:181-190`)。
> - 动态回退所需的 ①label 在线快照 + ⑤收割扇入,**无任何扩展点对应**,必须改核心或引入外部 operator。

### 3.2 必须改 Proxy + Broker

动态回退依赖 **流量标在线快照**(哪些 label 存在、是否在线),这是有状态的集群视图,必须:

- **Proxy 侧**:基于 `ClusterConsumerManager` + `HeartbeatSyncer` 维护跨实例的 label 在线快照,随 POP 请求把 `consumerLabel` + 在线快照下发给 Broker。
- **Broker 侧**:在 `PopMessageProcessor` 收敛单粒度路由决策。

两方案都建立在这个 Proxy+Broker 改造基线上,差异只在 Broker 如何处理"被跳过的隔离消息"。

### 3.3 源码验证结论(影响选型的硬事实)

落地前对照源码确认,以下事实已固定:

| 事实 | 锚点 | 影响 |
|---|---|---|
| offset 记账 key = `topic@group` | `ConsumerOffsetManager.java:201,241` | 方案 B 子游标用 **虚拟订阅组 `G%label`** 落地,复用现成记账,无需新 offset 表 |
| **POP 路径不支持 LMQ**(`PopMessageProcessor` 零 `isLmq`,LMQ 仅接 PULL) | `PopMessageProcessor.java`、`LmqPullRequestHoldService` | **LMQ 候选已推翻**,详见 [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] §0 |
| 逐条投递/跳过落在 `messageFilter`,游标照常前进 | `PopMessageProcessor.java:774-776` | 两方案的"按 label 决策"都挂 filter,不动游标推进 |
| 虚拟组在 `findSubscriptionGroupConfig` 会被拒 | `PopMessageProcessor.java:308` | 方案 B 必改点:虚拟组继承父组配置 |

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
| 子游标载体 | route topic(旧方案思路) | **虚拟订阅组 `G%label`**(复用 `topic@group` 记账;~~LMQ 已推翻~~) |

## 5. 共性问题(两方案都要处理)

- **抖动窗口跨环境泄漏**:隔离标在线/离线判定有快照延迟,窗口内可能误投。
- **at-least-once 重复投递**:收割 / 旁路切换的残余窗口需消费端幂等(RocketMQ 本就要求)。
- **流量标在线快照** 的准确性与同步延迟,是两方案共同的可用性关键。
- **retry topic + revive 异步孤儿**:隔离环境失败消息进虚拟组专属 retry topic(`%RETRY%G%gray1`),revive 异步重投。回收虚拟组前必须确认 retry topic 已读尽且 revive 无残留,否则孤儿(方案 B 专属,详见 plan-b §4c)。
- **broker 主从切换 / 重启**:收割队列、宽限期计时器是内存派生态。对齐 RocketMQ 现成范式(`PopBufferMergeService` 切 slave 即 clear、reviveOffset 持久同步),零持久化、挂 `changeSpecialServiceStatus`、冷启动重建,不丢消息(方案 B 专属,详见 plan-b §4d)。
- **同名重建语义(临时环境高频,用户拍板:接管上一代积压)**:gray 销毁后同名重建需**接管自己上一代积压**。机制:游标 `G%label` **跨代持久共享、不带 epoch**,重建后从现存游标续上;宽限期作为"接管 vs 回退"统一旋钮(宽限期内回来→全量接管,超时→标准收割剩余)。误删进度由回收临界区核对在线快照防护(方案 B 专属,详见 plan-b §4e)。
  - ⚠️ 张力:同一条消息不能既"等 gray 回来"又"立即给标准",故接管语义下**回退被延迟 = 宽限期长度**,这是该选择的固有代价。

## 6. 选型(2026-06-29 已定)

**确定前提(用户拍板)**:隔离环境 **临时**(PR预览/压测,频繁创建销毁);gray 离线后消息**优先等同名 gray 重建接管,宽限期内未回来才回退标准**(§4e 接管语义)。

由此:

- **Plan-B-Lite 静态版出局** —— 销毁的 gray 不会回来,纯虚拟组不收割会导致积压永久无人消费。
- **完整 plan-b 收割版选定** —— 见 [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] §4/§4a/§4b。
- 临时环境逼出的硬点:
  1. **宽限期 `gracePeriodMs`**:接管 vs 回退的统一旋钮 —— 宽限期内 gray 回来则从持久游标全量接管,超时才启动标准收割(§4a、§4e)。
  2. **持久共享游标(去 epoch)**:游标 `G%grayX` 跨代持久,重建的同名 gray 从现存游标续上积压(§4e)。
  3. **虚拟组回收**:收割完成后回收 `G%grayX` 元数据,防止"创建-销毁"循环导致膨胀(§4b)。**回收判定必须是三条件**(origin/retry offset 双双读尽 ∧ revive 无 in-flight checkpoint),否则 revive 异步重投会制造 retry topic 孤儿(§4c);并在**回收临界区核对在线快照**防误删接管者进度(§4e)。
- 收割驱动:**Broker 扇入 + 待收割轮转捎带**,正常路径零放大,收割时恒 1+1(§4 决策 A)。

> 方案 A([[2026-06-29-traffic-label-routing-plan-a-strict-bypass]])保留为对照:仅当 label 数量极大致虚拟组膨胀时才回头考虑。

### 下一步
- [x] 收敛 plan-b 为可实施设计(组件边界 §10.1、数据流 §10.2 已完成,见 [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] §10)
- [x] 按 planning 规则补 E2E / API 测试用例设计(5 条 E2E + API 合约表,见 [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] §11)
- [ ] design → 写 plan → 实施
