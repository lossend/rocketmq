---
name: traffic-label-routing-plan-b-subcursor-harvest
description: 方案 B —— 虚拟订阅组子游标 + 标准收割:用 G%label 虚拟组承载每个流量标,标准接管离线隔离标的虚拟组
date: 2026-06-29
status: brainstorming
verified: 已对照源码验证(ConsumerOffsetManager / PopMessageProcessor / LMQ 路径)
---

# 方案 B:虚拟订阅组子游标 + 标准收割

> 顶层设计见 [[2026-06-29-traffic-label-routing-design]]。
> 对照方案见 [[2026-06-29-traffic-label-routing-plan-a-strict-bypass]]。

## 0. 源码验证结论(本轮新增,决定落地形态)

落地前对照了 RocketMQ 源码,三个事实直接重塑了本方案:

| 事实 | 锚点 | 对方案的影响 |
|---|---|---|
| offset 记账 key = `topic@group`,每 `(group, queueId)` 一个游标 | `ConsumerOffsetManager.java:201,241` | "per-label 子游标"最干净的落地 = **把 label 编码进 group(虚拟订阅组 `G%label`)**,offset/checkpoint/revive/retry 全部自动复用,无需新建 offset 表 |
| **POP 路径完全不支持 LMQ**,`PopMessageProcessor` 零 `isLmq` 引用;LMQ 只接 PULL(`LmqPullRequestHoldService`) | `PopMessageProcessor.java`(全文无 isLmq);`broker/.../longpolling/LmqPullRequestHoldService.java` | **❌ 推翻上一版"用 LMQ 当子游标"的候选** —— 走 LMQ 需先把整个 POP 消费链改造成支持 LMQ,代价远大于虚拟组 |
| 逐条投递/跳过的落地点 = `messageFilter`,filter 跳过的消息游标照常前进(`nextBeginOffset`) | `PopMessageProcessor.java:774-776`、`appendCheckPoint :831` | "按 label 投递/SKIP"挂在 filter 上即可,**不动游标推进逻辑** |

> ⚠️ LMQ 候选已废弃,理由见上表第 2 行。后续不再考虑 LMQ。

## 1. 载体:虚拟订阅组 = 子游标

不自造 offset 表,而是把每个流量标编码成一个 **虚拟订阅组**,复用现成的 `topic@group` 记账:

| 角色 | 真实/虚拟组 | message filter | 谁来 POP |
|---|---|---|---|
| 标准消费者 | 真实组 `G` | `label == STANDARD` | 标准消费者 |
| gray1 在线 | 虚拟组 `G%gray1` | `label == gray1` | gray1 消费者 |
| gray1 离线 | 虚拟组 `G%gray1` | `label == gray1` | **标准消费者(收割)** |

**核心规则:标准组 `G` 永远只消费 `STANDARD`;每个 grayX 的全部消息恒由虚拟组 `G%grayX` 承载。grayX 在线时自己 POP `G%grayX`,离线时标准接管 POP `G%grayX`。**

这是相比"标准 filter 动态放行离线 label"更收敛的设计:标准组 G 的 filter 恒定不变,不受在线快照抖动影响;抖动只影响"标准要不要去 POP 某虚拟组",判断错了最多延迟或重复(at-least-once + 幂等兜底),**绝不丢消息**。

## 2. 所有权转移,而非消息转移

上一版"反例 B 卡死"在虚拟组模型下根本不存在:

```
queue: [stdA, gray1B, stdC]

gray1 在线:
  gray1 POP  G%gray1, filter=gray1 → 读 B,G%gray1 游标记账 B
  标准  POP  G,       filter=STANDARD → 读 stdA/stdC(B 不属于 G,与 G 游标无关)

gray1 离线:
  标准收割轮 POP G%gray1, filter=gray1
    ├ 情况① gray1 读了 B 未 ack 就崩 → B 在 G%gray1 的 checkpoint
    │        → revive 重投 %RETRY%G%gray1 → 标准收割 retry 队列拿到 ✓
    └ 情况② gray1 还没读到 B → 标准从 G%gray1 游标继续读到 B ✓
  两种情况 100% 复用 POP normal + retry + revive,零特判
```

关键:**B 从不归标准组 G 管**,B 的账一直记在 `G%gray1` 上;在线归 gray1、离线归标准 —— 转移的是"谁来 POP 这个虚拟组"的所有权,不是消息本身。

## 3. 改造点清单(带锚点)

1. **`PopMessageRequestHeader`**:新增 `consumerLabel` + `onlineLabelsSnapshot`(或快照版本号,Broker 缓存)。
2. **Proxy**:基于 `ClusterConsumerManager` + `HeartbeatSyncer` 维护 label 在线快照;路由:
   - gray 消费者 → 映射到 `G%label`。
   - 标准消费者 → 真实组 G + 下发"离线 label 列表"驱动收割。
3. **`PopMessageProcessor.processRequest`**:虚拟组 `G%grayX` 会在 `:308 findSubscriptionGroupConfig` 因组不存在被拒。**必改点**:让虚拟组继承父组 G 的 `SubscriptionGroupConfig`(自动补偿)。
4. **message filter 构建**(`:326-356`):注入 label 维度属性过滤(`__RMQ_TRAFFIC_LABEL`)。
5. **收割调度**:Broker 维护待收割队列,标准 POP 响应 round-robin 捎带一个待收割组,**走完整 `popMsgFromTopic`(origin + retry)**(§4 决策 A、§4c 约束 1)。
6. **宽限期**:新增 `gracePeriodMs` 配置;label 离线计时,超时才入待收割队列。宽限期 = 接管 vs 回退的统一旋钮(§4a、§4e)。
7. **虚拟组回收**:**三条件判定**(origin offset==max ∧ retry offset==max ∧ revive 无 in-flight ck,§4c 约束 2)+ **回收临界区核对在线快照**(§4e)后,回收 `G%grayX` 三处 offset + 订阅补偿 + retry topic 配置(§4b)。
8. **并发**:`popMsgFromQueue` 的 lockKey 已是 `topic#group#queueId`(`:695`),虚拟组天然隔离;gray1 离线判定与突然回线的竞态,靠该锁串行 + 快照 + 宽限期 + 消费端幂等兜底。
9. **主从切换挂载**:收割服务 + 宽限期计时器挂到 `BrokerController.java:2402 changeSpecialServiceStatus`,仅 master 运行,切 slave 即停清空,切回 master 冷启动重建(§4d)。
10. **持久共享游标(接管语义)**:游标 `G%label` 跨代持久,**不带 epoch**;gray 重建首次 POP 复用现存游标续上积压,不触发 `getInitOffset` 初始化(§4e)。

## 4. 已定决策(2026-06-29 用户拍板:隔离环境临时 + 要求回退)

> 前提锁定:隔离环境是**临时的**(PR预览/压测,频繁创建销毁),且**要求 gray 离线后消息回退标准**。
> 直接结论:[[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] §9 的 Plan-B-Lite 静态版**出局**,必须完整收割版。

### 决策 A:收割驱动 = Broker 扇入 + 待收割轮转捎带

朴素"标准 POP 时扇入全部离线组"在临时环境会放大成 1+N(N 抖动)。改进:

- Broker 维护**待收割队列**(进入条件见 §4a 宽限期)。
- 标准消费者正常 POP 只读真实组 `G`(**零放大**)。
- Broker 在标准 POP 响应里 **round-robin 捎带一个待收割组** `G%grayX` 的消息,单次放大恒为 **1+1**。
- 收割完成(§4b)的组移出待收割队列。

### 决策 B:生命周期 = 临时,收割为刚需(见 §4a/§4b)

## 4a. 临时环境硬点:离线 ≠ 销毁(宽限期)

临时环境下 gray 消失有两种,**在线快照无法区分**:

| 类型 | 含义 | 正确处理 |
|---|---|---|
| 抖动/重启 | 短暂离线,马上回来 | **不立即收割**,否则与回来的 gray 抢消费 → 重复 |
| 销毁 | 永久消失 | 收割干净 + 回收虚拟组(§4b) |

**解法:宽限期 `gracePeriodMs`**。gray 离线持续超过 T 才进入待收割队列。T 是**接管 vs 回退的统一旋钮**(§4e):
- 宽限期内 gray 回来 → 从持久游标 `G%grayX` 续上,**完全接管积压**。
- 宽限期到 → 标准开始收割,回退启动。T = 典型重建时间(PR 重新部署通常几分钟)。
- T 太小 → 真实重建来不及,积压过早被标准收割,gray 回来只能接管剩余(已收割部分不回头)。
- T 太大 → 真销毁后回退延迟高,`G%grayX` 积压久。

gray 在宽限期内回线 → 直接移出待收割,从游标续上,无副作用。

## 4b. 虚拟组回收(临时环境必需)

不回收则"创建-销毁"循环使 `G%grayX` 元数据无限膨胀。

> ⚠️ **回收判定不能只看 origin offset** —— retry topic + revive 异步会制造孤儿,详见 §4c。正确判定为三条件同时满足。

```
gray 销毁 → 离线超 gracePeriodMs → 进入待收割队列
         → 标准 round-robin 捎带【完整 POP】G%grayX(origin + retry,见 §4c 约束 1)
         → 三条件全满足(§4c)
         → 回收临界区:持锁 (G, grayX) 重新核对在线快照(§4e)
            ├ grayX 已回线 → 放弃回收,游标留给它接管
            └ 仍离线 → 回收 G%grayX 三处 offset + 订阅补偿 + retry topic 配置
```

回收只针对**真正销毁**的 gray;若 gray 在宽限期内回来,游标被它接管(§4e),回收不触发。

## 4c. retry topic 未消费消息的处理(本轮新增,源码验证)

gray1 消费失败的消息不在 origin topic,而在它**虚拟组专属的 retry topic** `%RETRY%G%gray1`。三个源码事实决定了处理方式:

| 事实 | 锚点 | 含义 |
|---|---|---|
| retry topic 按 cid(=consumerGroup)命名,虚拟组天然独占 | `KeyBuilder.java:32-41`(`%RETRY%` + cid + sep + topic) | `G%gray1` 的失败消息进 `%RETRY%G%gray1`,与标准组 `G` 的 retry 隔离,互不串 |
| 一次 POP 天然同时拉 origin + retry(随机先后) | `PopMessageProcessor.java:532-561` | 收割若走**完整虚拟组 POP**,retry 消息自动被捎带消费,**无需为 retry 单独写收割逻辑** |
| revive 是**异步延迟**重投:失败消息 checkpoint 先进 revive 队列,超时后才 `putMessage` 到 retry topic | `PopReviveService.java:113-159` | 收割完 origin 的瞬间,失败消息可能还卡在 revive 队列没重投到 retry → 此刻回收会制造孤儿 |

### 约束 1:收割必须走完整 POP

收割 `G%grayX` 复用现成 `popMsgFromTopic`(origin + retry 都拉),**不得优化成"只扫 origin consume queue"**,否则 retry topic 的未消费消息永远收不到。复用反而简化:retry 自动覆盖,零额外逻辑。

### 约束 2:回收判定 = 三条件(关键)

原"只看 origin offset==maxOffset"是**错的**。反例:gray1 崩溃瞬间,B 的 checkpoint 还在 revive 队列没重投,origin 已读完、retry 仍空 —— 若据此回收,几秒后 revive 把 B 重投到 `%RETRY%G%gray1`,而虚拟组已回收、无人 POP → **B 永久孤儿**。

正确回收判定(同时满足):

```
① origin G%gray1        : offset == maxOffset
② retry  %RETRY%G%gray1 : offset == maxOffset
③ revive 队列无 G%gray1 的 in-flight checkpoint(无待重投)
```

③ 是关键防线:必须等 revive 把该组所有 checkpoint 消化完(重投到 retry 或确认),retry topic 才算真空。回收时清三处:`G%gray1@origin`、`%RETRY%G%gray1` 的 offset、相关 checkpoint 残留。

## 4d. broker 主从切换 / 重启(内存态重建,源码验证)

收割服务的待收割队列、宽限期计时器都是**内存态**。结论:**对齐 RocketMQ 现成范式,零持久化,切换即清空、按持久真相重建**——不另起一套持久化。

### 现成范式(四个源码事实)

| 事实 | 锚点 | 含义 |
|---|---|---|
| revive 进度**持久化**(系统组 offset,随主从同步) | `PopReviveService.java:84`(`queryOffset` 初始化 reviveOffset) | 重启/切换后在途失败消息的 revive 进度不丢,继续重投到 `%RETRY%G%grayX` —— 关键安全垫 |
| POP 内存态设计成**可丢弃**:切 slave 直接 `buffer.clear()` | `PopBufferMergeService.java:74-101`(`isShouldRunning` + clear) | 内存 buffer 只是加速层,真相在持久 offset/checkpoint store;敢清空 |
| 统一主从切换钩子 | `BrokerController.java:2402 changeSpecialServiceStatus`(通知 schedule/transaction/**PopReviveService**) | 我们新增收割服务的**唯一正确挂载点** |
| 消费者在线表是非持久内存态,靠心跳重建 | `ConsumerManager.java:44 consumerTable`、`scanNotActiveChannel` | label 在线快照天然随之重建,无需持久化 |

### 设计原则:派生态零持久化

待收割队列 + 宽限期计时器不是"真相",只是"待办索引"。真相是持久的:

| 内存态 | 重启/切换后 | 持久真相来源 |
|---|---|---|
| 待收割队列 | 丢弃,冷启动重推导 | "虚拟组 offset 存在 + 无在线消费者" |
| 宽限期计时器 | 重新计时(从 0) | label 在线状态由心跳重建,最坏多等一个宽限期 |
| 收割进度 | 无需恢复 | `G%grayX` 的 offset 已持久(`topic@group` 记账) |
| 失败消息 revive | 自动继续 | reviveOffset 持久 + 主从同步 |

### 三条规则

- **规则 1（角色感知）**:收割服务 + 宽限期计时器只在 master 跑,挂到 `changeSpecialServiceStatus`,切 slave 即停并清空 —— 完全比照 `PopBufferMergeService`。
- **规则 2（冷启动重建）**:切回 master 后扫描所有 `G%label` 虚拟组 offset,对照当前在线快照,离线者重新进入宽限期(从 0 计)。最坏代价:一次额外宽限期延迟,**不丢消息**。
- **规则 3（收割中途崩溃）**:已 POP 未 ack 的收割消息,checkpoint 已在持久 store,新 master 的 revive 重投到 `%RETRY%G%grayX`,下一轮收割捡回。**与 §4c 三条件回收天然自洽**:revive 没清完不会判定回收。

## 4e. 同名重建:接管上一代积压(2026-06-29 用户拍板)

> **语义决定(用户拍板)**:同名 gray 重建后**接管自己上一代的积压**,而非回退给标准。
> 这与"gray 离线→回退标准"存在直接张力(见下),需用宽限期作为统一旋钮调和。

### 核心张力:同一条消息不能既"等 gray 回来"又"立即给标准"

对 gray1 离线期间到达的消息 M:
- **接管积压** → 必须留着 M 等 gray1 回来。
- **回退标准** → 必须立即把 M 给标准。

二者对同一条 M 互斥。**选择接管 = 接受回退被延迟**,无法回避。

### 机制:去掉 epoch,游标按 label 持久共享

> ⚠️ **推翻上一版的 epoch 隔离**:epoch(`G%gray1%e2`)把同名两代物理隔离,恰恰**阻止接管**。要接管就**去掉 epoch**,游标只按 label 命名 `G%gray1`、**跨代持久共享**。

游标 `G%gray1` 持久且共享(gray1 在线时它消费,离线时标准收割它):

```
gray1 离线
  └─ 宽限期内:标准【不收割】(冻结),积压留给 gray1
       ├ gray1 回来 → 从 G%gray1 续上 → 拿到 100% 积压 ✓ 完全接管
       └ 宽限期到 → 标准开始收割 G%gray1(回退启动)
            └ gray1 更晚回来 → 从 G%gray1 续上(共享游标)
              → 拿到标准【尚未收割】的剩余部分,已被标准消费的不再给(at-least-once,已处理不丢)
```

- **回退延迟 = 宽限期长度**。宽限期设成典型重建时间(PR 环境重新部署通常几分钟):快速重建拿全量,真死了几分钟后回退标准。
- gray1 重建首次 POP 发现 `G%gray1` 游标**还在**(未被清)→ offset 不<0 → **不走 `getInitOffset` 初始化** → 直接从积压处续上。**上一版"坏结局 1 静默丢失"(`max-1` 跳历史,`PopMessageProcessor.java:941`)因此不再触发。**

### 坏结局 2(回收误删进度)的新防线:回收临界区

去 epoch 后不能再靠物理隔离防误删,改为**回收原子核对**:

```
回收前持锁 (G, gray1) → 重新核对在线快照
  ├ gray1 已在线 → 放弃回收,让它接管
  └ 仍离线 ∧ §4c 三条件成立 → 才删 G%gray1
```

残余亚毫秒边界竞态由 at-least-once + 消费端幂等兜底(系统本就要求)。

### 回收仍然需要(只是更晚)

接管语义不取消回收 —— 真正销毁的 gray 仍要回收元数据,否则膨胀(§4b)。区别:回收只在"宽限期到 + 标准收割干净 + 核对仍离线"后发生。若 gray 在此前回来,游标被它接管,回收自然不触发。

## 5. 优点 / 缺点

**优点**
- **不复制消息体**:无 route topic、无旁路存储、无 route index。
- **零 revive 特判**:失败重试复用每条虚拟组的 POP retry / revive。
- **复用现成 offset 记账**:虚拟组走 `topic@group`,无新存储结构。
- **正常路径零放大**:标准 POP 只读 G,收割靠 round-robin 捎带,单次放大恒 1+1。

**缺点 / 风险**
- **虚拟组元数据膨胀**:offset 记录数 = label 数 × queue 数。临时环境靠 §4b 回收控制;label 极多时仍倾向 [[2026-06-29-traffic-label-routing-plan-a-strict-bypass]]。
- **宽限期权衡**:`gracePeriodMs` 太小→抖动误判致重复;太大→回退延迟高(§4a)。
- **收割并发竞争**:gray 回线与标准收割的竞态,需锁 + 宽限期 + 幂等兜底。
- **回退实时性中等**:依赖宽限期 + 收割轮转,非即时。
- **在线快照强依赖**:标准必须准确知道"哪些 label 存在且离线"才能收割。
- **主从切换冷启动延迟**:切回 master 后宽限期从 0 重计,最坏多等一个 `gracePeriodMs` 才恢复收割,期间不丢消息(§4d)。

## 6. 简化版(隔离环境常驻)—— ⚠️ 本项目不适用

> 本项目隔离环境为**临时**(§4 决策 B),本节仅作对比保留,不采用。

若隔离环境 **常驻、仅偶尔抖动**:可退化为 **纯虚拟组、不收割**。抖动时 gray1 回来自己继续 POP `G%gray1`,不串环境,几乎零额外机制。临时环境不能用此简化版 —— 销毁的 gray 不会回来,积压消息永久无人消费。

## 7. 适用场景

- **本项目(临时 + 要求回退)→ 完整收割版**(§4/§4a/§4b),已选定。
- 隔离环境常驻灰度 → 简化版(纯虚拟组),本项目不适用。
- label 数量极大 → 虚拟组膨胀使本方案退化,倾向 [[2026-06-29-traffic-label-routing-plan-a-strict-bypass]]。

## 8. 扩展点可行性分析(回应"尽量少侵入")

把 plan-b 拆成 5 个必需能力,逐一对照 [[Server_Extension_Points]](`docs/cn/Server_Extension_Points.md`)中的扩展点:

| 必需能力 | 现有扩展点能否承载 | 锚点 | 结论 |
|---|---|---|---|
| ① label 在线快照(集群视图) | 无;`ClusterConsumerManager`/`HeartbeatSyncer` 是核心组件,需加 label 维度 | — | 改核心 |
| ② POP 路由改写(group→`G%label`) | Proxy `RequestPipeline` 能拦截,但 ①非 SPI(pipeline 硬编码注册,无配置类名装载)②是 1→1,做不了扇入 | `proxy/.../grpc/pipeline/RequestPipeline.java` | 改核心 |
| ③ 虚拟组可被 POP | 无扩展点伪造订阅组 | `PopMessageProcessor.java:308` | 改核心 **或** 预创建真实组(运维) |
| ④ 按 label 过滤/SKIP | Proxy `PopMessageResultFilter` 的 `NO_MATCH` 会 **ACK 消息→丢消息**,不可用于回退;Store `MessageFilter` 可 SKIP 但注入点非 SPI;**SQL92 `ExpressionMessageFilter` 在 POP 路径现成可用** | `ConsumerProcessor.java:181-190`(陷阱)、`PopMessageProcessor.java:326`(SQL92 透传) | ✅ SQL92 零侵入 |
| ⑤ 收割扇入(标准读 `G%grayX`) | 无扩展点;`ConsumerProcessor.popMessage` 是 1→1 | `ConsumerProcessor.java:79` | 改核心 **或** 外部 operator |

> ⚠️ **陷阱**:Proxy `PopMessageResultFilter` 看似能"按 label 丢弃",但 `NO_MATCH` 分支直接 `ackMessage`(`ConsumerProcessor.java:181-190`),消息被确认消费、永久消失,**无法回退**。不能用于本需求。

**结论:完全零侵入(扩展点+配置+运维)只能拿到"静态隔离",拿不到"动态回退"。** 回退依赖 ①在线快照 + ⑤收割,这两个核心能力无任何扩展点对应。

## 10. 组件边界与数据流(2026-06-29 新增)

### 10.1 组件边界图

```
┌──────────────────────────────────────────────────────────────────┐
│                          Proxy                                    │
│                                                                  │
│  ┌──────────────────────────┐  ┌────────────────────────────┐   │
│  │   LabelSnapshotManager   │  │         POPRouter           │   │
│  │                          │  │                             │   │
│  │ consumerTable            │─▶│ gray consumer               │   │
│  │ Map<label,Set<instance>> │  │   → group G → G%label       │   │
│  │                          │  │ std consumer                │   │
│  │ 心跳超时 → label offline  │  │   → 真实组 G                │   │
│  │ 触发 GracePeriodTimer     │  │   + offlineLabels[] 下发   │   │
│  └──────────────────────────┘  └────────────┬───────────────┘   │
│           ▲ consumer heartbeat               │ POP request header │
└───────────┼──────────────────────────────────┼───────────────────┘
            │ register/heartbeat               │ gRPC ReceiveMessage
            │                                  ▼
┌──────────────────────────────────────────────────────────────────┐
│                          Broker                                   │
│                                                                  │
│  ┌───────────────────┐  ┌──────────────────┐                    │
│  │ VirtualGroup      │  │ LabelMsgFilter   │                    │
│  │ Compensator       │  │                  │                    │
│  │                   │  │ G%grayX:         │                    │
│  │ :308 G%grayX →    │  │  label == grayX  │                    │
│  │  继承父组 G config │  │ G(std):          │                    │
│  └───────┬───────────┘  │  label==STD      │                    │
│          │              │  OR IS NULL      │                    │
│          │              └──────┬───────────┘                    │
│          │                     │                                │
│          ▼                     ▼                                │
│  ┌────────────────────────────────────────────────────────┐     │
│  │              PopMessageProcessor                        │     │
│  │                                                        │     │
│  │  popMsgFromTopic(origin + retry 同时拉)                │     │
│  │  lockKey = topic#group#queueId                         │◀────┼─ HarvestScheduler
│  │  offset → ConsumerOffsetManager(topic@group)           │     │  round-robin 捎带
│  └────────────────────────────────────────────────────────┘     │
│                                                                  │
│  ┌──────────────────────────┐  ┌──────────────────────────────┐ │
│  │    GracePeriodTimer      │  │  VirtualGroupLifecycle       │ │
│  │                          │  │  Manager                     │ │
│  │ label 离线 → 计时 T      │  │                              │ │
│  │ 超时 → 入 harvestQueue   │  │ 三条件判定(§4c)              │ │
│  │ gray 回线 → 取消计时     │  │ 回收临界区:                  │ │
│  │                          │  │  lock(G,grayX)               │ │
│  │ 挂 changeSpecialService  │  │  → 重核在线快照              │ │
│  │ Status(master-only)      │  │  → 删 offset/config(§4e)     │ │
│  └──────────────────────────┘  └──────────────────────────────┘ │
│                                                                  │
│  ┌──────────────────────────┐                                   │
│  │   HarvestScheduler       │                                   │
│  │                          │                                   │
│  │ harvestQueue             │                                   │
│  │ round-robin 取一         │                                   │
│  │ → piggyback 捎带进标准   │                                   │
│  │   POP 响应(恒 1+1)       │                                   │
│  └──────────────────────────┘                                   │
└──────────────────────────────────────────────────────────────────┘
```

**接口约定:**

| 边界 | 接口 | 数据 |
|---|---|---|
| Proxy → Broker POP header | `PopMessageRequestHeader` | `consumerLabel`, `onlineLabelsSnapshot`(版本号或完整快照) |
| Broker → Proxy POP 响应 | `PopMessageResponse` | 正常消息 + piggyback 收割消息(统一 `MessageExt` list) |
| LabelSnapshotManager → GracePeriodTimer | 事件信号 | `(label, offline/online, timestamp)` |
| GracePeriodTimer → HarvestScheduler | 入队信号 | `label` |
| HarvestScheduler → PopMessageProcessor | 收割参数 | `group=G%grayX, topic` |
| VirtualGroupLifecycleManager → ConsumerOffsetManager | 删除 | `topic@G%grayX`, `topic@%RETRY%G%grayX` |

### 10.2 三条核心数据流

#### Flow 1:gray1 在线正常消费

```
gray1 consumer ──POP G──▶ Proxy POPRouter
  │ 映射 G → G%gray1, consumerLabel=gray1
  ▼
Broker PopMessageProcessor(group=G%gray1)
  │ VirtualGroupCompensator → 继承父组 G config(:308)
  │ LabelMsgFilter → filter: label==gray1 (:326)
  │ popMsgFromTopic(G%gray1, origin + retry)
  │ ConsumerOffsetManager → G%gray1@topic cursor 前进
  ▼
返回 label==gray1 的消息给 gray1 consumer
```

#### Flow 2:gray1 离线 → 标准收割

```
gray1 离线 → LabelSnapshotManager 感知
  │ GracePeriodTimer 开始计时(gracePeriodMs)
  │   ├ 宽限期内 gray1 回来 → 取消计时,回 Flow 1
  │   └ 超时 → HarvestScheduler.enqueue(gray1)

std consumer ──POP G──▶ Proxy POPRouter
  │ group=G, offlineLabels=[gray1]
  ▼
Broker PopMessageProcessor(group=G)
  │ LabelMsgFilter → filter: label==STANDARD OR IS NULL
  │ HarvestScheduler round-robin → 取出 G%gray1
  │ popMsgFromTopic(G%gray1, origin + retry) ← 完整 POP
  │ ConsumerOffsetManager → G%gray1@topic cursor 前进
  ▼
POP 响应:标准消息 + piggyback gray1 收割消息(1+1)
  └ 标准 consumer 处理两批消息,各自 ACK

收割持续 → 三条件满足(§4c) → VirtualGroupLifecycleManager
  │ lock(G, gray1) + 重核在线快照
  │   ├ gray1 回来 → 放弃回收
  │   └ 仍离线 → 删 G%gray1 offset + retry config
  ▼
harvestQueue 移出 gray1,回收完成
```

#### Flow 3:同名 gray1 重建 → 接管积压

```
gray1 重建 → 心跳到 Proxy
  │ LabelSnapshotManager 标记 gray1 online
  │ HarvestScheduler 移出 G%gray1(若在队中)
  │ GracePeriodTimer 取消(若在计时)

gray1 ──POP G──▶ Proxy POPRouter → G%gray1
  ▼
Broker PopMessageProcessor(group=G%gray1)
  │ ConsumerOffsetManager.queryOffset(topic@G%gray1)
  │   → offset 存在(游标跨代持久)
  │   → offset ≠ -1 → 不走 getInitOffset(:941)
  │   → 直接从上次位置续上
  ▼
gray1 从积压处继续消费,已被标准收割的部分不再给(at-least-once)
标准消费者同步感知 gray1 回线 → 停止捎带 G%gray1 收割
```

## 11. E2E / API 测试用例设计(2026-06-29 新增)

> 按 planning 规则:每条核心用户流至少一个 E2E。测试不 mock 依赖服务,使用真实 Broker + Proxy(Testcontainers 或本地部署)。

### 11.1 三类核心流程测试矩阵

| 测试编号 | 流程类型 | 核心断言 |
|---|---|---|
| E2E-01 | 回退流程:gray 离线后标准接管 | 无消息丢失,gray 离线后宽限期结束标准消费到 gray 消息 |
| E2E-02 | 接管流程:同名 gray 重建后继承积压 | 重建的 gray 从上次游标续上,标准不消费 gray 消息 |
| E2E-03 | 销毁回收:gray 销毁后元数据被清理 | 全部消息被消费,`G%gray1` offset 记录被回收 |
| E2E-04 | 并存隔离:多个 gray 互不干扰 | gray1 消息只进 gray1,gray2 消息只进 gray2 |
| E2E-05 | 抖动防护:短暂离线不触发收割 | gray 在宽限期内回来,标准未消费任何 gray 消息 |

### 11.2 E2E-01:回退流程

```
前提:
  - Broker + Proxy 启动,topic=test-topic,queueNum=4
  - 标准消费者 group=G,SQL92: label IS NULL OR label='STANDARD'
  - gray1 消费者 group=G%gray1,SQL92: label='gray1'
  - gracePeriodMs=10s

步骤:
  1. gray1 消费者上线,heartbeat 建立
  2. 发送 20 条 label=gray1 的消息(M1-M20)
  3. 断言:gray1 消费 M1-M20,标准未消费任何一条
  4. 停止 gray1 消费者(模拟下线)
  5. 等待 5s(宽限期内)→ 发送 10 条 label=gray1(M21-M30)
  6. 断言:5s 内标准未消费 M21-M30(宽限期保护)
  7. 等待 gracePeriodMs + 5s(宽限期到)
  8. 发送 10 条 label=gray1(M31-M40)
  9. 断言:标准消费 M21-M40(含宽限期积压 + 新消息),总数 = 40

断言明细:
  - gray1 消费计数 = 20
  - 标准消费计数 = 20
  - 消息不重复(幂等性由 msgId 校验)
  - 无消息丢失:生产总数 40 = 消费总数 40
```

### 11.3 E2E-02:接管流程(同名重建)

```
前提:gracePeriodMs=15s

步骤:
  1. gray1 消费者上线,消费 M1-M10
  2. 停止 gray1 消费者(游标在 M10 位置)
  3. 立刻发送 20 条 label=gray1(M11-M30)
  4. 等待 8s(宽限期内,< 15s)
  5. 断言:标准未消费 M11-M30(宽限期保护)
  6. **重建 gray1 消费者(同名 group=G%gray1)**
  7. 等待 gray1 消费者消费完毕
  8. 断言:gray1 从 M11 续上消费(接管积压,不从 M1 重放)
  9. 断言:标准未消费任何 M11-M30
  10. 断言:生产总数 30 = gray1 消费 30(M1-M30)

关键验证点:
  - gray1 重建后游标位置 = M10 之后(共享游标未被重置)
  - 标准消费者 M11-M30 消费计数 = 0
```

### 11.4 E2E-03:销毁回收

```
前提:gracePeriodMs=10s,回收检查间隔=5s

步骤:
  1. gray1 消费者上线,发送 30 条 label=gray1
  2. gray1 消费 15 条成功(M1-M15),5 条消费失败(M16-M20,nack 进 retry),未消费 M21-M30
  3. 停止 gray1 消费者(**不重建**)
  4. 等待 gracePeriodMs → 标准开始收割
  5. 等待收割完成(所有消息被标准消费,含 retry 中的 M16-M20)
  6. 等待 revive 处理完毕(revive 周期结束)
  7. 等待三条件满足 + 回收触发

断言明细:
  - 标准消费计数 = 15(M16-M30,含 retry)
  - 总消费 = 30(gray1 消费 15 + 标准消费 15)
  - 回收后:ConsumerOffsetManager 中 `test-topic@G%gray1` 记录不存在
  - 回收后:`test-topic@%RETRY%G%gray1` 记录不存在
  - 无孤儿消息:retry topic `%RETRY%G%gray1` 中 offset == maxOffset
```

### 11.5 E2E-04:多 gray 并存隔离

```
步骤:
  1. gray1 消费者(G%gray1)+ gray2 消费者(G%gray2)+ 标准消费者(G)全部上线
  2. 各发送 10 条:label=gray1(M-g1)、label=gray2(M-g2)、label=STANDARD(M-std)
  
断言:
  - gray1 消费且仅消费 M-g1(10 条)
  - gray2 消费且仅消费 M-g2(10 条)
  - 标准消费且仅消费 M-std(10 条)
  - 无跨环境污染
```

### 11.6 E2E-05:抖动防护(宽限期内回线)

```
前提:gracePeriodMs=20s

步骤:
  1. gray1 在线,发送 20 条 label=gray1
  2. gray1 停止(模拟 GC/重启抖动)
  3. 等待 10s(< gracePeriodMs)→ gray1 重新上线
  4. 等待 gray1 消费完毕

断言:
  - 标准消费 M-gray1 计数 = 0(宽限期内标准未收割)
  - gray1 消费计数 = 20(完全接管)
  - harvestQueue 中 gray1 未出现(或出现后被移出)
```

### 11.7 API 测试:POP 请求/响应合约

| 用例 | 请求 | 预期响应 |
|---|---|---|
| gray consumer 正常 POP | `consumerLabel=gray1`, `onlineLabelsSnapshot={gray1:online}` | 仅返回 `label=gray1` 消息 |
| std consumer 正常 POP | `consumerLabel=STANDARD`, `offlineLabels=[]` | 仅返回 `label=STANDARD OR IS NULL` 消息 |
| std consumer harvest POP | `consumerLabel=STANDARD`, `offlineLabels=[gray1]` | 标准消息 + piggyback `G%gray1` 消息(1+1) |
| 虚拟组 POP(无配置) | `group=G%gray1`(config 不存在) | 自动继承 G config,正常返回(不返回 `GROUP_NOT_EXIST`) |
| 在线快照版本命中 | `snapshotVersion=N`(与 Broker 缓存匹配) | 复用 Broker 缓存,无需传全量快照 |

## 9. 最小侵入变体:Plan-B-Lite(零 broker 侵入)

能力 ④ 的 SQL92 发现,使"静态隔离"可做到零 broker 代码改动:

1. 每个隔离环境**预创建真实订阅组** `G%gray1`(mqadmin,零代码)。
2. **gray 消费者**:group=`G%gray1`,SQL92 订阅 `__RMQ_TRAFFIC_LABEL = 'gray1'`。
3. **标准消费者**:group=`G`,SQL92 订阅 `__RMQ_TRAFFIC_LABEL IS NULL OR __RMQ_TRAFFIC_LABEL = 'STANDARD'`。
4. broker 开 `enablePropertyFilter`(配置项)。

不同 group = 独立游标(解决 [[2026-06-29-traffic-label-routing-design]] §2 的单游标张力),SQL92 = 现成单粒度过滤。**全程不改 broker/proxy 一行代码。**

### 缺口:动态回退

Plan-B-Lite 缺的正是回退 —— gray1 离线后 `G%gray1` 积压,标准订阅 STANDARD 不会碰。补回退两条路:

| 补法 | 机制 | broker 侵入 | 代价 |
|---|---|---|---|
| **A. 改核心加收割** | 回到完整 plan-b(虚拟组 + 标准扇入收割) | 有(集中在 Proxy 路由 + `PopMessageProcessor:308` 虚拟组补偿,可控) | 改动可控,回退实时 |
| **B. 外部 operator** | 监控 gray 在线,离线时动态让标准实例加入 `G%gray1` 改订阅收割,上线时踢出 | **零** | 新增外部组件 + 回退慢 + 上下线竞态 |

### 选型建议

- 隔离环境**常驻** → Plan-B-Lite 静态版几乎够用(真零侵入)。
- 必须**动态回退** → 改核心的收割(补法 A)比外部 operator 更可靠,侵入比想象中小。
