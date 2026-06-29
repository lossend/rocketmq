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

1. **`PopMessageRequestHeader`**:新增 `consumerLabel`(含 epoch,即 `label%epoch`,§4e)+ `onlineLabelsSnapshot`(或快照版本号,Broker 缓存)。
2. **Proxy**:基于 `ClusterConsumerManager` + `HeartbeatSyncer` 维护 label 在线快照;路由:
   - gray 消费者 → 映射到 `G%label%epoch`。
   - 标准消费者 → 真实组 G + 下发"离线 `label%epoch` 列表"驱动收割。
3. **`PopMessageProcessor.processRequest`**:虚拟组 `G%grayX%epoch` 会在 `:308 findSubscriptionGroupConfig` 因组不存在被拒。**必改点**:让虚拟组继承父组 G 的 `SubscriptionGroupConfig`(自动补偿)。
4. **message filter 构建**(`:326-356`):注入 label 维度属性过滤(`__RMQ_TRAFFIC_LABEL`)。
5. **收割调度**:Broker 维护待收割队列,标准 POP 响应 round-robin 捎带一个待收割组,**走完整 `popMsgFromTopic`(origin + retry)**(§4 决策 A、§4c 约束 1)。
6. **宽限期**:新增 `gracePeriodMs` 配置;`label%epoch` 离线计时,超时才入待收割队列(§4a)。
7. **虚拟组回收**:**三条件判定**(origin offset==max ∧ retry offset==max ∧ revive 无 in-flight ck,§4c 约束 2)后,回收 `G%grayX%epoch` 三处 offset + 订阅补偿 + retry topic 配置(§4b)。
8. **并发**:`popMsgFromQueue` 的 lockKey 已是 `topic#group#queueId`(`:695`),虚拟组天然隔离;gray1 离线判定与突然回线的竞态,靠该锁串行 + 快照 + 宽限期 + 消费端幂等兜底。
9. **主从切换挂载**:收割服务 + 宽限期计时器挂到 `BrokerController.java:2402 changeSpecialServiceStatus`,仅 master 运行,切 slave 即停清空,切回 master 冷启动重建(§4d)。
10. **epoch 分配**:隔离环境创建时由部署系统分配 epoch(实例唯一 ID / 启动时间戳),消费者上报时随 `consumerLabel` 携带;收割/回收均按 `label%epoch` 粒度,根除同名重建竞态(§4e)。

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

**解法:宽限期 `gracePeriodMs`**。gray 离线持续超过 T 才进入待收割队列。T 是临时环境特有参数,平衡回退延迟与重复率:
- T 太小 → 抖动被误判销毁,gray 回来时部分消息已被标准收割 → 重复(靠消费端幂等兜底)。
- T 太大 → 真销毁后回退延迟高,`G%grayX` 积压久。

gray 在宽限期内回线 → 直接移出待收割,无副作用。

## 4b. 虚拟组回收(临时环境必需)

不回收则"创建-销毁"循环使 `G%grayX` 元数据无限膨胀。

> ⚠️ **回收判定不能只看 origin offset** —— retry topic + revive 异步会制造孤儿,详见 §4c。正确判定为三条件同时满足。

```
gray 销毁 → 离线超 gracePeriodMs → 进入待收割队列
         → 标准 round-robin 捎带【完整 POP】G%grayX(origin + retry,见 §4c 约束 1)
         → 三条件全满足(§4c)→ 判定收割完成
         → 回收 G%grayX 三处 offset + 订阅补偿 + retry topic 配置
```

回收需幂等:若回收后 gray 同名重建,虚拟组按首次 POP 重新补偿即可(§3.3)。

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

## 4e. 同名重建竞态:epoch 隔离(源码验证,含丢失风险)

临时环境最高频场景:gray1 销毁后**同名重建**(PR 关了又开)。回收对 `G%gray1` 做删除,新实例首次 POP 做初始化,两个写操作交错,有两种坏结局:

### 坏结局 1:消息静默丢失(严重,与回收无关)

`getInitOffset`(`PopMessageProcessor.java:941`)默认 initMode 下,无 offset 记录时初始化为 **`maxOffsetInQueue - 1`(只读最后一条,跳过全部历史)**:

```
t0 老 gray1 销毁,G%gray1 积压 [B,C,D,E] 未收割
t1 同名 gray1 重建,首次 POP → offset<0 → getInitOffset 默认 = max-1
   → 只能看到 E,[B,C,D] 被永久跳过 → 丢失
```

比孤儿更隐蔽:只要"同名重建 + offset 已清"就触发,与回收是否发生无关。

### 坏结局 2:回收删掉新实例进度

```
t0 回收判定 G%gray1 收割完成
t1 新 gray1 首次 POP,commit offset=X
t2 回收执行 removeConsumerOffset(topic@G%gray1) → 删掉 X(ConsumerOffsetManager.java:68)
t3 新 gray1 下次 POP → offset<0 → 又被当首次,重置 → 重复/丢失
```

### 根因与解法:虚拟组带 epoch

根因:同名 label 复用,新旧生命周期状态无法区分。`ConsumerManager` 的补偿过期靠 `subVersion` 时间戳(`ConsumerManager.java:338`),能过期清理但**不能区分同名两代**。

解法:**虚拟组带 epoch** → `G%gray1%<epoch>`。epoch 由隔离环境创建时分配(部署系统的实例唯一 ID / 启动时间戳,**不是 label 名**),消费者 POP 时随 `consumerLabel` 一起下发;收割与回收都按 `label%epoch` 粒度。

| 问题 | epoch 如何根除 |
|---|---|
| 坏结局 1 丢失 | 新实例是 `G%gray1%e2`,全新虚拟组,offset<0 走首次初始化是**正确的**(本就该从自己上线点起);老 `G%gray1%e1` 的积压 `[B,C,D]` 仍挂 e1,由标准收割 → 不丢 |
| 坏结局 2 误删 | 回收删 `G%gray1%e1`,新实例用 `G%gray1%e2`,**物理隔离,不可能交错** |

把"同名重建竞态"从竞态降级成"两个不同虚拟组",**根除而非缓解**。

### 待你确认的语义权衡

用 epoch 后,新实例 e2 **不消费**老实例 e1 遗留的 `[B,C,D]` —— 它们走收割**回退给标准**。这符合 plan-b 回退语义(gray 离线→回标准),且重建的 e2 是全新环境、不应继承上一代脏数据。**若期望"同名 gray 重建后接管老实例遗留",则 epoch 方案不满足**(需另设计 label 级 offset 继承,复杂度高、且与回退语义冲突)。当前设计取**回退给标准**。

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
