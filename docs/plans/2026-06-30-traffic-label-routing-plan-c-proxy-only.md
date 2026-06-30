---
name: traffic-label-routing-plan-c-proxy-only
description: 方案 C —— 只改 Proxy、无回退:消费者声明 label,Proxy 透明改写组名 + SQL92,admin 懒建真实组 G%label,broker 零代码
date: 2026-06-30
status: brainstorming
verified: 已对照源码验证(ReceiptHandle / ReceiveMessageActivity / DefaultAdminService / MQClientAPIImpl)
---

# 方案 C:只改 Proxy、无回退

> 本文是在 [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] 基础上的**简化派生**:
> **砍掉「回退标准」能力**,改用 Proxy 扩展能力,**只改 Proxy、broker 零代码**。
> 顶层设计与方案索引见 [[2026-06-29-traffic-label-routing-design]]。
>
> ⚠️ 本文不修改原 plan-b,plan-b 作为「需要动态回退」的完整版保留。

## 0. 一句话定位

> 每个流量标 = 一个**真实订阅组** `G%label`,各自独立游标,只消费自己的 label。
> Proxy 把消费者「声明的 label」翻译成「组名 + SQL92 过滤」,并经 admin 懒建组。
> **没有回退,所以没有在线快照、收割、宽限期、回收 —— 全套机制随回退一起砍掉。**

## 1. 与 plan-b 的差异:砍掉了什么(决定本方案形态)

砍掉「gray 离线后消息回退标准」这一条,直接消灭了 plan-b 绝大部分复杂度。下表逐项说明**每个机制存在的唯一理由**,以及为何回退砍掉后它就失去存在意义:

| plan-b 机制 | 存在的唯一理由 | 本方案 | 锚点(plan-b) |
|---|---|---|---|
| 在线快照 `LabelSnapshotManager` | 驱动「哪些 label 离线 → 需收割」 | ✂️ 砍掉 | plan-b §10.1 |
| 收割 `HarvestScheduler` + 标准扇入 | 让标准接管离线 gray 的积压 | ✂️ 砍掉 | plan-b §4 决策 A |
| 宽限期 `GracePeriodTimer` | 「接管 vs 回退」的统一旋钮 | ✂️ 砍掉(无回退即无旋钮) | plan-b §4a |
| 虚拟组回收 + 三条件判定 + 回收临界区 | 临时环境防元数据膨胀 | ✂️ 砍掉(任其过期) | plan-b §4b/§4c/§4e |
| broker `PopMessageProcessor:308` 虚拟组继承父组配置 | 让虚拟组能被 POP | ✂️ 砍掉(改用真实组) | plan-b §3 改造点 3 |
| `PopMessageRequestHeader` 新增 `consumerLabel`/快照 | 下发快照驱动收割 | ✂️ 砍掉 | plan-b §3 改造点 1 |
| 主从切换冷启动重建 | 收割服务是内存派生态 | ✂️ 砍掉(无收割服务) | plan-b §4d |

**结果:broker 零代码改动(仅开 `enablePropertyFilter` 配置项),所有逻辑收敛到 Proxy 消费端。**

### 1.1 代价(明确承认)

砍掉回退是有代价的,本方案明确接受:

- **gray 离线/销毁期间打了 `label` 的消息会堆在 `G%label` 游标里,无人消费。** 标准不会来收割。
- 这些积压靠 **topic 消息 TTL 自然过期**(用户拍板「任其过期」)。
- `G%label` 真实组元数据**永久保留**,不回收。临时环境若 label 极多,组配置数会持续增长 —— 这是已知且接受的代价(若不可接受,回到 plan-b 或方案 A 的运维回收)。

## 2. 决策锁定(2026-06-30 用户拍板)

| # | 决策点 | 选择 | 影响 |
|---|---|---|---|
| D1 | 虚拟组在 broker 端如何存在 | **Proxy 经 admin 懒建真实组**(继承 G 配置) | broker 零代码,无 `GROUP_NOT_EXIST` 陷阱 |
| D2 | Proxy 如何知道请求属于哪个 label | **消费者声明 label**(client 属性/环境),Proxy 从请求上下文读 | 消费者业务代码无感知 |
| D3 | 离线积压 + 组元数据如何收场 | **默认任其过期**(消息 TTL 过期,组永久保留);**可选定时清理**(配置开关,默认关) | 默认最简;清理为可选轻量回收(见 §2.1 配置) |
| D4 | label 翻译放在哪 | **方案 A:Proxy 透明改写**(消费者仍连 group=G) | 消费者零感知,改写收敛在 Proxy |
| D5 | 生产侧打标是否纳入本设计 | **否**,假设消息已带 `__RMQ_TRAFFIC_LABEL` | 本文只管消费路由 |
| D6 | 隔离环境 consumer 发现机制 | **不使用**(参考文档仅作背景) | 路由纯靠请求上下文声明 label |
| D7 | 功能默认状态(生产安全) | **整体默认关闭**,由总开关控制;清理、路由日志各自独立开关,均默认关 | 避免在生产误用;仅隔离环境显式开启(见 §2.1) |

> D6 参考文档 [[2026-06-30-consumer-clientinfo-index-by-topic]] 提供的是事件驱动的 consumer 索引。本方案**不需要发现** —— 每个 consumer 自己声明 label,Proxy 看请求即可决定路由,不依赖「别的 label 在不在线」。

## 2.1 配置项(生产安全:默认全关)

三个 `ProxyConfig` 配置项,**默认值均使整套功能在生产环境处于关闭/无副作用状态**:

| 配置项 | 类型 | 默认 | 作用 | 关闭时行为 |
|---|---|---|---|---|
| `enableTrafficLabelRouting` | boolean | **`false`** | **总开关**。开启后 Proxy 才读声明 label、做组改写、SQL92 注入、懒建组。 | 完全旁路 —— 所有消费侧请求按原生逻辑(group=G 原样透传),与未引入本功能完全等价 |
| `enableTrafficLabelGroupCleanup` | boolean | **`false`** | 定时清理隔离环境相关元数据:`G%label` 订阅组 + `%RETRY%G%label` retry 队列。 | 不清理,`G%label` 组与 retry topic 永久保留(§6 默认行为) |
| `enableTrafficLabelRoutingLog` | boolean | **`false`** | 输出隔离路由相关日志(改写决策、懒建组、清理动作),便于隔离环境排障。 | 不输出路由日志,避免生产日志噪音 |

### 总开关语义(`enableTrafficLabelRouting`)

- **关闭(默认,生产)**:`ConsumerSideGroupRewriter` / `LabelRoutingResolver` / `LabelGroupBootstrapper` 全部短路返回,消费侧请求零改写。**等价于本功能不存在**,这是「避免在生产上误用」的硬保证。
- **开启(隔离环境)**:进入 §4 完整路由逻辑。
- 总开关关闭时,其余两个子开关无意义(被总开关短路)。

### 清理开关语义(`enableTrafficLabelGroupCleanup`)

这是把 plan-b 回收能力以**可选、轻量**形式引回 —— 但**不需要 plan-b 的三条件判定**(因为本方案无标准收割,不存在 revive 异步孤儿与标准接管竞态):

```
定时任务(仅当 enableTrafficLabelRouting && enableTrafficLabelGroupCleanup):
  扫描所有 G%label 真实组
    对每个 G%label:
      ├ 当前有在线消费者(该 label 有 consumer 连接)→ 跳过
      ├ G%label origin offset == maxOffset
      │   ∧ %RETRY%G%label offset == maxOffset
      │   ∧ 持续离线超过 cleanupIdleThresholdMs(防抖动误删)
      │   → admin 删除 G%label 订阅组 + %RETRY%G%label retry 队列
      └ 否则 → 跳过
```

- 判定比 plan-b 简单:**无第三条件「revive 无 in-flight checkpoint」** —— 本方案不做标准收割,gray 自己的 revive 在它自己的虚拟真实组内闭环,删组前 offset 双双读尽已足够(失败消息要么已被 gray 重试消费、要么已过期)。
- 仍保留**离线时长阈值**防抖动:短暂离线的 gray 不会被误删(同名重建仍能接管,§6)。
- 清理用 admin RPC(删组复用 `DeleteSubscriptionGroup`,删 retry topic 复用现成 admin 能力),**仍不改 broker**。
- ⚠️ 与「同名重建接管积压」(§6)的张力:清理删组后,同名 gray 重建将**从初始 offset 开始**(组被删,游标丢失)。故清理阈值应设得足够大(典型重建周期之上),或在确认 gray 真销毁后才开启。默认关闭即回避此张力。


## 3. 源码验证结论(决定落地形态)

落地前对照 Proxy 源码,四个事实直接重塑本方案:

| 事实 | 锚点 | 对方案的影响 |
|---|---|---|
| **`createSubscriptionGroup` RPC 客户端已存在**,发 `UPDATE_AND_CREATE_SUBSCRIPTIONGROUP` 给 broker;broker handler 也已存在(mqadmin 建组走的就是这条路),建的是**真实持久组**,自动主从同步 | `client/.../MQClientAPIImpl.java:431`(`createSubscriptionGroup`) | D1 可行:Proxy 只需在 `AdminService` 加薄方法转调,**不动 broker** |
| Proxy 已持有 admin 客户端,但 `AdminService` 接口目前**只暴露建 topic**,没暴露建 group | `proxy/.../service/admin/DefaultAdminService.java:37`、`AdminService.java:23` | 唯一接口扩展:给 `AdminService` 加 `createSubscriptionGroup(...)` |
| **`ReceiptHandle.encode()` 不编码组名**(只有 startOffset/queueId/brokerName/offset 等);ack/changeInvisibleTime 的组名是**每次从客户端请求重新取的** | `common/.../consumer/ReceiptHandle.java:43-47`;`ReceiveMessageActivity.java:105` `request.getGroup().getName()` | **关键约束**:客户端始终发 group=G,Proxy 必须在**每个消费侧调用**(receive/ack/changeInvisible/DLQ)做同一次 `G→G%label` 改写,否则 ack 回错组、消息无法确认 |
| 组名提取点集中:receive 在 `ReceiveMessageActivity:105`,ack/changeInvisible/DLQ 在各自 activity 同样从 `request.getGroup()` 取 | `ReceiveMessageActivity.java:103-105` | 四处改写可收敛到**一处工具方法**统一调用 |

## 4. 组件设计

### 4.1 三个 Proxy 组件

| 组件 | 职责 | 挂点 | 状态 |
|---|---|---|---|
| **LabelRoutingResolver** | 给定 `(group=G, declaredLabel)` → 输出 `effectiveGroup` + `SQL92 exp`。<br>gray:`G%gray1` + `__RMQ_TRAFFIC_LABEL='gray1'`;<br>标准(无 label):`G` + `__RMQ_TRAFFIC_LABEL IS NULL OR ='STANDARD'`。 | 工具类 | 无状态 |
| **ConsumerSideGroupRewriter** | 在 receive/ack/changeInvisible/DLQ 四个消费侧入口统一调用 Resolver,改写组名 + 注入/合并 SQL92。**保证四处一致**(因 ReceiptHandle 不带组,见 §3 第 3 行)。 | `ReceiveMessageActivity` + 3 个 ack 类 activity | 无状态 |
| **LabelGroupBootstrapper** | 首次路由到 `G%gray1` 时经 `AdminService.createSubscriptionGroup(G%gray1, 继承 G 配置)` 懒建真实组;本地缓存已建集合避免重复 admin 调用。 | 新增 `AdminService` 薄方法,调已存在的 `MQClientAPIImpl.createSubscriptionGroup` | 内存缓存(可丢弃,丢了最多重复建组一次,幂等) |

### 4.2 组件边界图

```
gray1 consumer ──(group=G, 声明 label=gray1)──┐
std   consumer ──(group=G, 无 label)──────────┤
                                               ▼
┌─────────────────────────── Proxy ───────────────────────────┐
│                                                              │
│  ┌────────────────────────┐   ┌──────────────────────────┐  │
│  │ LabelRoutingResolver   │   │ ConsumerSideGroupRewriter │  │
│  │ (group=G, label)       │──▶│ receive / ack /           │  │
│  │  → effectiveGroup       │   │ changeInvisible / DLQ     │  │
│  │  → SQL92 exp(AND 合并) │   │ 四处统一改写组名+过滤     │  │
│  └────────────────────────┘   └────────────┬─────────────┘  │
│           │ 首次见 G%gray1                  │                 │
│           ▼                                 │                 │
│  ┌────────────────────────┐                 │                 │
│  │ LabelGroupBootstrapper │                 │                 │
│  │ admin.createSubscription│                │                 │
│  │ Group(G%gray1 继承 G)  │                 │                 │
│  │ + 已建缓存             │                 │                 │
│  └────────────────────────┘                 │                 │
└──────────────────────────────────────────────┼───────────────┘
              │ createSubscriptionGroup RPC      │ POP/ACK group=G%gray1
              │ (已存在,UPDATE_AND_CREATE_SUB)   │ exp=SQL92(label='gray1')
              ▼                                  ▼
┌─────────────────────────── Broker(零代码改动) ──────────────────┐
│  enablePropertyFilter=true(配置项)                              │
│  G%gray1 是真实组 → findSubscriptionGroupConfig 天然通过         │
│  独立 topic@G%gray1 游标(ConsumerOffsetManager 现成记账)        │
│  失败消息 → %RETRY%G%gray1(真实组天然独占,复用 POP retry/revive) │
└──────────────────────────────────────────────────────────────────┘
```

### 4.3 接口约定

| 边界 | 接口 | 数据 |
|---|---|---|
| Consumer → Proxy 声明 label | gRPC client 属性 / 环境 | `__RMQ_TRAFFIC_LABEL=gray1`(从 `ProxyContext`/`Settings` 读) |
| Proxy 内部 Resolver → Rewriter | 方法返回 | `(effectiveGroup, sql92Exp)` |
| Proxy → Broker(receive/ack/...) | 已有 RPC,改写后的 group/exp | `group=G%gray1`, `exp=label='gray1'` |
| Proxy → Broker 建组 | `MQClientAPIImpl.createSubscriptionGroup`(已存在) | `SubscriptionGroupConfig(groupName=G%gray1, 继承 G)` |

## 5. label 声明通道 & SQL92 合并(必须正确)

- **声明通道**:消费者通过 gRPC client 属性(`__RMQ_TRAFFIC_LABEL`)或系统环境声明 label。Proxy 从 `ProxyContext` / `Settings` 读取,业务代码无感知(仅启动配置)。
- **SQL92 合并(关键正确性)**:消费者自身可能已带订阅过滤表达式。Resolver 必须用 **AND 合并** label 条件,**不能覆盖**消费者原表达式。
  - gray:`(消费者原 exp) AND __RMQ_TRAFFIC_LABEL = 'gray1'`
  - 标准:`(消费者原 exp) AND (__RMQ_TRAFFIC_LABEL IS NULL OR __RMQ_TRAFFIC_LABEL = 'STANDARD')`
  - 消费者无原 exp 时,直接用 label 条件。
- **broker 配置**:`enablePropertyFilter=true`(配置项,非改码),启用 SQL92 属性过滤。

## 6. 消费模型 & 各路径后果

| 场景 | 路径 | 结果 |
|---|---|---|
| gray1 在线 | 自己 POP `G%gray1`,独立游标 | 只拿 `label=gray1` 消息 |
| 标准在线 | POP `G`,SQL92 补集过滤 | 只拿 `STANDARD`/null 消息 |
| gray1 失败重试 | 失败消息进 `%RETRY%G%gray1` | 复用现成 POP retry/revive,**零特判**(真实组独占 retry) |
| gray1 离线/销毁 | `label=gray1` 消息堆 `G%gray1` 游标 | **无人消费**(无回退、无收割),靠 TTL 过期 |
| 同名 gray1 重建 | `G%gray1` 真实组游标持久还在(未被清理) | 从积压处**续上**(接管);若清理已删组则从初始 offset 起 |
| 元数据(清理关,默认) | `G%gray1` 真实组配置 | **永久保留**,任其存在 |
| 元数据(清理开,可选) | 离线 + offset 双读尽 + 超时长阈值 | 定时删 `G%gray1` 组 + `%RETRY%G%gray1`(§2.1) |

## 7. 改造点清单(带锚点)

0. **`ProxyConfig` 配置项**(新增三项,§2.1):`enableTrafficLabelRouting`、`enableTrafficLabelGroupCleanup`、`enableTrafficLabelRoutingLog`,默认全 `false`。所有下列组件入口先判总开关,关闭即短路。
   - 锚点:`proxy/.../config/ProxyConfig.java`
1. **`AdminService` 接口 + `DefaultAdminService`**:新增 `createSubscriptionGroup(String groupName, SubscriptionGroupConfig config)`,内部调 `MQClientAPIImpl.createSubscriptionGroup`(已存在)。清理还需 `deleteSubscriptionGroup` + 删 retry topic 的薄方法(复用现成 admin RPC)。
   - 锚点:`proxy/.../service/admin/AdminService.java:23`、`DefaultAdminService.java:37`、`client/.../MQClientAPIImpl.java:431`
2. **LabelRoutingResolver**(新增工具类):`(group, label, 原 exp)` → `(effectiveGroup, mergedSql92)`。
3. **ConsumerSideGroupRewriter**(新增):在四个消费侧 activity 入口统一改写;**入口先判 `enableTrafficLabelRouting`,关闭则原样透传**。
   - receive 锚点:`proxy/.../grpc/v2/consumer/ReceiveMessageActivity.java:103-105`
   - ack / changeInvisibleDuration / forwardToDLQ:对应 activity 的 `request.getGroup()` 提取点
4. **LabelGroupBootstrapper**(新增):懒建组 + 已建缓存。
5. **LabelGroupCleaner**(新增,可选):定时任务,仅当 `enableTrafficLabelRouting && enableTrafficLabelGroupCleanup` 运行;扫描 + 判定(§2.1)+ admin 删组/删 retry。
6. **路由日志**:在改写决策 / 懒建组 / 清理动作处按 `enableTrafficLabelRoutingLog` 输出。
7. **Proxy 启动装配**:在 `messagingProcessor.start()` 前注册/注入上述组件(Proxy 无 SPI,需显式装配);清理定时任务随 Proxy 生命周期启停。
   - 锚点参考:[[2026-06-30-consumer-clientinfo-index-by-topic]] 的启动注册范式
8. **broker 配置**:`enablePropertyFilter=true`(部署配置,非代码)。

> **不改 broker 任何一行代码。** 与 plan-b 改造点清单(10 项,含多处 broker)对比,本方案 broker 项为 0。

## 8. 优点 / 缺点

**优点**
- **broker 零代码**:仅开 `enablePropertyFilter` 配置项,所有逻辑在 Proxy。
- **生产安全**:总开关 `enableTrafficLabelRouting` 默认关,关闭时完全旁路,等价本功能不存在(§2.1)。
- **复用现成能力**:真实组走 `topic@group` 记账、POP retry/revive、admin 建组 RPC,全部现成。
- **零回退机制**:无在线快照 / 收割 / 宽限期 / 主从冷启动重建,复杂度比 plan-b 低一个量级。
- **消费者透明**(方案 A):消费者仍连 group=G,只声明 label,组名/SQL92/建组全在 Proxy。
- **同名重建天然接管**:游标持久 + 默认不清理 → 重建直接续上积压。
- **清理可选**:`enableTrafficLabelGroupCleanup` 按需开启轻量回收,判定比 plan-b 少一条件(§2.1)。

**缺点 / 风险**
- **无回退**:gray 离线/销毁期间的 `label` 消息无人消费,依赖 TTL 过期(已知接受)。
- **清理 vs 接管张力**:开启清理后,删组会使同名重建丢失游标、从初始 offset 起。靠离线时长阈值 + 默认关闭回避(§2.1)。
- **组元数据不回收(清理关时)**:label 极多时 `G%label` 配置持续增长(已知接受;可开清理缓解)。
- **四处改写一致性**:receive/ack/changeInvisible/DLQ 必须用同一改写逻辑,漏一处则 ack 回错组导致消息无法确认 —— 主要实现风险,靠收敛到单一工具方法 + 测试覆盖防护。
- **SQL92 合并正确性**:必须 AND 合并而非覆盖消费者原表达式。
- **抖动窗口**:声明 label 本身无快照延迟,但消费者重建瞬间若标准与 gray 短暂并存,靠 SQL92 互斥过滤(label 精确匹配)天然隔离,无跨环境泄漏。

## 9. 与其他方案的关系

| 方案 | 回退 | broker 改动 | 复杂度 | 适用 |
|---|---|---|---|---|
| 本方案 C(只改 Proxy、无回退) | ❌ | **0** | 低 | 隔离环境可接受积压过期、追求最小侵入 |
| [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]](子游标+收割) | ✅ 动态回退 | 多处 | 中 | 必须回退、临时环境 |
| [[2026-06-29-traffic-label-routing-plan-a-strict-bypass]](旁路存储) | ✅ | 多处 | 高 | label 极多致虚拟组膨胀 |

> 选型分水岭:**是否必须「gray 离线后消息回退标准」**。
> - 需要 → plan-b(完整收割版)。
> - 不需要、可接受积压过期 → 本方案 C(只改 Proxy)。

## 10. E2E / API 测试用例设计

> 按 planning 规则:每条核心用户流至少一个 E2E。测试不 mock 依赖服务,使用真实 Broker + Proxy(Testcontainers 或本地部署),broker 开 `enablePropertyFilter=true`。

### 10.1 核心流程测试矩阵

| 编号 | 流程 | 核心断言 |
|---|---|---|
| E2E-01 | gray 在线隔离消费 | gray1 只消费 label=gray1,标准只消费 STANDARD/null,互不串 |
| E2E-02 | 多 gray 并存隔离 | gray1/gray2/标准 三方互不干扰 |
| E2E-03 | 懒建组 | gray1 首次连接触发 admin 建真实组 `G%gray1`,继承 G 配置 |
| E2E-04 | 失败重试隔离 | gray1 失败消息进 `%RETRY%G%gray1`,gray1 自己重试消费,不串标准 retry |
| E2E-05 | 同名重建接管积压 | gray1 离线留积压 → 同名重建从游标续上,不从头重放,标准不碰 |
| E2E-06 | 离线积压无回退(负向断言) | gray1 离线后,标准**始终不消费** label=gray1 消息(验证砍掉回退) |
| E2E-07 | ack 一致性 | gray1 消费后 ack 正确回到 `G%gray1`,消息不重投(验证四处改写一致) |
| E2E-08 | 总开关关闭旁路 | `enableTrafficLabelRouting=false` 时,gray 声明 label 也按原生 group=G 消费,无改写、无懒建组 |
| E2E-09 | 可选清理 | 开启清理后,真销毁 gray 的 `G%label` 组 + `%RETRY%G%label` 在阈值后被删;在线 gray 不被删 |

### 10.2 E2E-01:gray 在线隔离消费

```
前提:Broker+Proxy 启动,enablePropertyFilter=true,topic=test-topic,queueNum=4
  - gray1 消费者:连 group=G,声明 label=gray1
  - 标准消费者:连 group=G,无 label

步骤:
  1. 两消费者上线
  2. 发 10 条 label=gray1 + 10 条 label=STANDARD + 10 条无 label
步骤断言:
  - gray1 消费计数 = 10(全部 label=gray1)
  - 标准消费计数 = 20(STANDARD + 无 label)
  - 无交叉:gray1 未消费任何 STANDARD/null;标准未消费任何 gray1
```

### 10.3 E2E-03:懒建组

```
步骤:
  1. 确认 broker 无 G%gray1 组(mqadmin 查无)
  2. gray1 消费者首次连接(group=G, label=gray1)
  3. Proxy 触发 LabelGroupBootstrapper
断言:
  - broker 出现真实订阅组 G%gray1
  - G%gray1 配置继承自 G(retryMaxTimes/retryQueueNum 等关键项一致)
  - 第二次 gray1 连接不再重复发建组 RPC(命中本地缓存)
```

### 10.4 E2E-05:同名重建接管积压

```
步骤:
  1. gray1 上线,消费 M1-M10
  2. 停止 gray1(游标在 M10)
  3. 发 20 条 label=gray1(M11-M30,堆在 G%gray1)
  4. 等待一段时间(无宽限期概念,直接观察)
  5. 断言:标准未消费任何 M11-M30(无回退)
  6. 同名重建 gray1(连 group=G, label=gray1)
  7. 等待消费完毕
断言:
  - gray1 从 M11 续上(游标未重置,接管积压)
  - gray1 不从 M1 重放
  - 标准 M11-M30 消费计数 = 0
  - 生产总数 30 = gray1 消费 30
```

### 10.5 E2E-06:离线积压无回退(负向断言,验证砍掉回退)

```
步骤:
  1. gray1 上线消费,标准上线
  2. 停止 gray1(不重建)
  3. 发 15 条 label=gray1
  4. 持续等待(超过任何典型宽限期窗口)
断言:
  - 标准消费 label=gray1 计数 = 0(永不回退)
  - 15 条积压留在 G%gray1 游标,等待 TTL 过期
  - 标准 STANDARD/null 消费不受影响
```

### 10.6 E2E-07:ack 一致性

```
步骤:
  1. gray1 上线(group=G, label=gray1)
  2. 发 5 条 label=gray1
  3. gray1 正常消费并 ack 全部 5 条
  4. 等待超过 invisibleTime
断言:
  - 5 条消息不重投(ack 正确回到 G%gray1,而非 G)
  - G%gray1 游标推进到 max
  - 验证点:抓 ack RPC,group 字段 = G%gray1
```

### 10.8 E2E-08:总开关关闭旁路(生产安全)

```
前提:enableTrafficLabelRouting=false(默认)

步骤:
  1. gray1 消费者连 group=G,声明 label=gray1
  2. 发 10 条 label=gray1 + 10 条 label=STANDARD
断言:
  - Proxy 未做任何组改写(抓 RPC,group 始终 = G)
  - 未触发懒建组(broker 无 G%gray1 组)
  - gray1 与标准按原生 group=G 共享同一游标消费(无隔离)
  - 行为与未引入本功能完全一致
```

### 10.9 E2E-09:可选清理

```
前提:enableTrafficLabelRouting=true, enableTrafficLabelGroupCleanup=true,
      cleanupIdleThresholdMs=10s,清理扫描间隔=5s

步骤:
  1. gray1 上线消费完所有 label=gray1(G%gray1 与 %RETRY%G%gray1 offset 均到 max)
  2. 停止 gray1(不重建)
  3. 等待 cleanupIdleThresholdMs + 一个扫描周期
断言:
  - G%gray1 订阅组被删(mqadmin 查无)
  - %RETRY%G%gray1 retry 队列被删
对照(防误删):
  4. 另起 gray2 保持在线
  5. 断言:G%gray2 不被清理(有在线消费者 → 跳过)
抖动对照:
  6. gray3 上线消费完,离线 5s(< 阈值)后重建
  7. 断言:G%gray3 未被删,重建从游标续上(阈值防抖动)
```

### 10.10 API 测试:消费侧请求改写合约

| 用例 | 客户端请求 | 预期 Proxy → Broker |
|---|---|---|
| gray receive | `group=G`, 声明 `label=gray1`, exp=空 | `group=G%gray1`, exp=`__RMQ_TRAFFIC_LABEL='gray1'` |
| 标准 receive | `group=G`, 无 label, exp=空 | `group=G`, exp=`__RMQ_TRAFFIC_LABEL IS NULL OR ='STANDARD'` |
| gray receive 带原过滤 | `group=G`, `label=gray1`, exp=`a>1` | `group=G%gray1`, exp=`(a>1) AND __RMQ_TRAFFIC_LABEL='gray1'`(AND 合并) |
| gray ack | `group=G`, handle(无组名) | ack RPC `group=G%gray1`(改写一致) |
| gray changeInvisible | `group=G`, handle | RPC `group=G%gray1` |
| 懒建组 | gray1 首次 receive | 先发 `createSubscriptionGroup(G%gray1)`,再 POP |
| 总开关关闭 | `enableTrafficLabelRouting=false`,gray1 receive | 原样 `group=G`,无改写、无懒建组 RPC |
