# RocketMQ Proxy 无 SDK 改造的优雅上下线重设计

> **实施要求：** 使用 `planning-with-files` 与 `executing-plans` skills 按阶段执行；任何实现开始前先重新读取本计划、`task_plan.md`、`findings.md` 和 `progress.md`。

> **参考关系与优先级：** 本文是 Server/Helm-only 方案的规范性计划；原文档 [`plan-proxy-graceful-lifecycle.md`](./plan-proxy-graceful-lifecycle.md) 仅作为 Proxy 实现参考。可复用其中生命周期协调器、CAS 准入门、双终态计数、资源逆序关闭、指标与全链路 `exec` 的思路；不得复用 SDK 双连接、`proxyInstanceId`、Drain Notice/ACK、新 Proto 或 120 秒时钟。两份文档冲突时一律以本文为准，原文档保持不改。

**目标：** 仅修改 RocketMQ Proxy Server 与 `/Users/lossend/pro/rocketmq-helm`，让计划内扩容、滚动重启和受控缩容对 acknowledged send 达到零观测错误/超时，并限制尾延迟增量。

**架构：** 使用稳定 L4 Service/NLB、5 分钟随机连接租约、统一 Proxy 生命周期状态机和 Kubernetes 长排空窗口。客户端不增加双连接、不增加 Drain Proto/ACK，也不引入 Gateway。

**技术栈：** Java 8、gRPC Java/Netty、RocketMQ Remoting、Kubernetes Deployment/Service/PDB（固定 replicas，不使用 HPA）、Helm、AWS/ACK NLB、OpenTelemetry/Prometheus。

---

## 1. 结论、边界与现状问题

本计划完整替换旧方案中的 SDK 双热连接、`proxyInstanceId`、Drain Notice/ACK 和新增 Proto；旧方案的 Proxy 侧原子准入与资源关闭设计只作为非规范性参考。核心模型调整为：

```text
稳定 L4 地址 + 周期性连接租约 + Proxy 分阶段排空 + Kubernetes 单 Pod 编排
```

当前主要问题：

- Proxy 没有统一的 `READY/QUIESCING/DRAINING` 状态、业务 readiness 和幂等排空入口。
- gRPC 没有 `maxConnectionAge`；长达数十年的 Telemetry 流会长期固定到单个 Pod。
- Remoting 只在全局 shutdown 时返回 GO_AWAY，不能进行日常连接轮换；Proxy 默认也未启用其优雅关闭。
- send 从入口、Broker Future 到响应写回没有统一闭环计数，固定 sleep 不能证明排空完成。
- 当前 `GrpcServer.shutdown()` 虽会经 `io.grpc.Server.shutdown()` 触发 grpc-java 内建双 GOAWAY，但把“发起有序关闭、固定时长等待、TLS listener 注销”揉进一个 `void` 方法；它忽略 `awaitTermination` 的 `false`、不调用 `shutdownNow()`、吞掉中断/失败，且没有回收 builder 自建的 boss/worker EventLoopGroup。
- Helm 使用 TCP 探针和立即 `mqshutdown` 的 PreStop；NLB 摘流、连接迁移和 JVM 终止窗口没有统一时钟。
- 容器启动链存在未 `exec` 的 shell，SIGTERM 不保证抵达 JVM。
- 主 Chart 的生产 PDB 值被模板硬编码为 1；Remoting 稳定访问地址也未强制配置。
- L4 长连接不会因为扩容自动迁移，新 Pod 只能承接新连接。

严格承诺仅覆盖：

- Kubernetes 计划内 rollout、restart 或脚本控制的逐 Pod scale-down（固定 replicas，不含 HPA 驱动的伸缩）。
- 有响应的同步/异步 send；`sendOneway` 不在严格范围内。
- RocketMQ 原有 at-least-once 语义；重连竞态产生的重复消息不算方案失败，消费者仍需幂等。

不承诺 SIGKILL、OOM、节点丢失、网络分区、Pod IP 直连或一次删除多个 Pod。

### 1.1 三类能力必须分开建模（复审 P0-1）

以下三者语义不同，**不得**合并成一个「GOAWAY 屏障」：

| 能力 | 语义 | 边界 |
|---|---|---|
| gRPC `Server.shutdown()` | 全局、一次性、释放 listener | **不可逆**；同一 `Server` 不能重新 start；不能在 READY 状态可逆调用 |
| Remoting `GO_AWAY` | 请求到达后的**响应**，非可广播屏障 | 仅 `cmd.version > V5_3_1` 且非 oneway；客户端只能 transport 层重放一次 |
| 应用层 `SendDrainGate` | 只管理已进入业务分发路径的请求 | **不等价于**连接迁移完成 |

由此产生三条硬约束：

- `awaitTermination()` 只证明 gRPC Server 终止，**不能**证明 Remoting 已无新请求；不得用作跨协议屏障。
- Remoting 在返回 `GO_AWAY` 前仍需读取请求，「关闭应用 gate」**不是**它的迁移 ACK。
- 每个协议的 no-new-work 边界是**待验证项**，必须由 §7 Step 1 的 spike 证明，不得用假想的「GOAWAY ACK」代替。

若 spike 证明公共 API 下无法同时满足「不接收新业务工作」「不主动拒绝仍可能合法到达的请求」「strict client 无 `UNAVAILABLE`」，必须在四条中显式选择并记录：放宽零失败目标并定义错误预算；修改客户端重试/`waitForReady` 契约；引入新 transport 能力或拆分 listener；调整 provider 摘流方式使新请求在 listener 关闭前可证明不再到达。

### 1.2 待关闭的范围决策（复审 P0-1、P0-3）

以下决策必须在编码前定稿，不得由实施阶段默认：

- **oneway**：当前协议下不能依赖 `GO_AWAY` 达成零丢失（`writeResponse()` 对 oneway 直接返回，shutdown 分支会跳过业务 dispatch 又不写出 `GO_AWAY`，形成静默丢失）。首期必须三选一：修改协议/客户端、排除或禁用 oneway、或证明 provider no-new-arrival 后再关闭 admission。
- **Remoting 策略矩阵**：按 `clientVersion × sync/async/oneway` 单独选择策略，不得一句话覆盖。
- **事务消息**：见 §9.4，四选一并记录。
- **消费语义（ReceiptHandle/redelivery）**：是否进入严格发布门禁属于范围决策，需产品范围说明后确认，不默认升为 P0。

## 2. 客户端版本分析与兼容契约

| 客户端 | 等级 | 已验证行为与设计含义 |
|---|---|---|
| `rocketmq-client-java:5.0.7` | 严格验收 | gRPC Java 1.50；每个 Endpoints 一个 ManagedChannel；`pick_first`；**显式 `disableRetry()`**；无 `waitForReady`；Producer 默认 3 次尝试、单次请求默认 3 秒。标准 GO_AWAY 会重建 transport；Telemetry response observer 的 `onError/onCompleted` 都在 1 秒后执行 `renewRequestObserver()` 并重发 Settings，因此正常 completion 可避免 `UNAVAILABLE` 错误日志。**`maxAttempts=3` 只是偶然缓冲，不是正确性屏障**（见下方风险说明）。 |
| `rocketmq-client-java:5.2.1` | 严格验收 | send 与 Telemetry 迁移能力和 5.0.7 相同；新增 300 秒 keepalive。`ReconnectEndpointsCommand` 只设置布尔标志，不关闭或重建 Channel，因此不能作为迁移机制；正常完成 Telemetry 后的 1 秒 observer renewal 才是本方案的恢复路径，`onError` 仅作异常兜底。 |
| `rocketmq-client:5.3.2+` | 严格验收 | Remoting 支持 GO_AWAY 后重连并透明重试；服务端现有版本门槛为 `> V5_3_1`。 |
| `rocketmq-client:5.2.0` 及更早 | 降级兼容 | 5.2.0 虽有重连代码，但当前服务端不会向其发送 GO_AWAY；只能在最终断链后依赖普通重连/Producer 重试。 |
| 经典 Remoting 5.2.1 | 不存在 | 官方 5.2.1 坐标属于 `rocketmq-client-java`；经典 `rocketmq-client` 没有 5.2.1 发布。 |

客户端行为证据固定来自 `/Users/lossend/opensource/rocketmq-clients` 的 `java-5.0.7`、`java-5.2.1` tags：`ClientSessionImpl.onError/onCompleted` 都以 `REQUEST_OBSERVER_RENEW_BACKOFF_DELAY=1s` 调度 `renewRequestObserver()`，renew 成功后调用 `syncSettings0()`；`RpcClientImpl.receiveMessage` 则把 `onError` 完成成异常、把 `onCompleted` 完成成当前响应列表。因此 drain policy 只对 Telemetry 使用 OK completion，不能把同一策略泛化给 ReceiveMessage。最终矩阵仍从正式 Maven 依赖启动独立 JVM，不能使用本地改造后的 SDK。

**strict client 零失败结论仍缺真实证据（复审 P0-4）。** 5.0.7 与 5.2.1 都调用 `disableRetry()`；没有 `waitForReady` 时 RPC 在 `TRANSIENT_FAILURE` 可立即失败，应用层 `maxAttempts=3` 又是无 backoff 的快速重试，可能仍命中同一个 channel/Endpoints。因此以下四句**不得**写入规范性结论：

- 「listener 关闭后的 TCP connect failure 只触发 subchannel 重连，不产生 RPC 失败」；
- 「gRPC GOAWAY 使所有 in-flight、新建和尚未分配 transport 的 RPC 都透明成功」；
- 「`maxAttempts=3` 可作为生命周期正确性屏障」；
- 「客户端一般会重连」可用于关闭 P0-1。

必须建立真实客户端矩阵（详见 §5.2），`maxAttempts=1` 会更早暴露竞态但不是唯一需覆盖的配置。矩阵失败时回到 §1.1 改协议、SDK、provider 或目标。

严格测试使用客户端默认或更高的 `maxAttempts=3`、至少 3 秒请求超时，并覆盖 `maxAttempts=1`；保持 Remoting `enableReconnectForGoAway=true`。不修改任何 SDK 源码或客户端协议。

## 3. Proxy 运行时重构

### 3.1 Runtime 所有权、生命周期与就绪

新增非静态 `ProxyRuntime` 作为进程内唯一资源所有者；`ProxyLifecycleCoordinator`、协议 Server、MessagingProcessor、管理服务和执行器全部由构造器注入并挂在该 runtime 下。禁止把 coordinator 放进 `ConfigurationManager` 或进程级 singleton；PreStop、SIGTERM hook 和两个协议必须引用同一个 runtime 实例，但 PreStop 使用 `DrainRun`、TERM 使用独立 `StopRun`，不能混成一个 completion future。

严格能力只用于 Cluster mode。Local mode 在总开关关闭时保持原关闭语义；若在 Local mode 开启严格生命周期则启动失败，防止产生虚假的优雅承诺。

状态只能单调转换，正常与强制路径明确分开：

```text
STARTING -> READY -> QUIESCING -> MIGRATING -> DRAINING -> DRAINED
                                                     -> STOPPING -> STOPPED
STARTING/READY/QUIESCING/MIGRATING/DRAINING
                         -> FORCE_DRAINING -> STOPPING -> STOPPED
```

- `STARTING`：先启动管理服务，再启动 MessagingProcessor、gRPC 和 Remoting；未接收过业务时收到 TERM 可跳过迁移，但仍走同一个 once-only 关闭器。
- `READY`：初始 warmup barrier 完成，readiness 成功并接收业务。
- `QUIESCING`：readiness 立即失败；send admission 仍对 cutoff 前的存量 transport 开放。
- `MIGRATING`：等待连接租约自然结束，使客户端通过稳定 Service/NLB 重建连接。
- `DRAINING`：关闭准入并冻结协议 intake，等待精确 send 终态和 transport 终止。
- `DRAINED`：业务协议已经安全终止，PreStop 可以返回。
- `FORCE_DRAINING`：唯一的 deadline 超时路径；必须记录原因、使本轮严格验收失败并执行有界强制关闭，不得伪装成 DRAINED。
- `STOPPING/STOPPED`：只由 SIGTERM hook 或启动失败关闭器触发；HTTP/PreStop 排空路径停在 DRAINED 或 FORCE_DRAINING，不主动推进 STOPPING。并发 PreStop 共享同一 drain future，重复 TERM 共享另一条 stop future，两者分别 once-only。

初始 readiness 使用一次性 `ReadinessContributor` barrier，至少要求：管理服务与两个业务 listener 已绑定、MessagingProcessor 和 ACL/TLS 已初始化、NameServer 首次同步成功、路由缓存至少成功刷新一次。`proxyWarmupTopics` 非空时，还要为每个关键 topic 完成 route lookup 和 Broker channel 建立；禁止发送真实探测消息。

warmup 超时后 Pod 保持 STARTING/NotReady、记录原因并退避重试，由 rollout timeout 阻止继续替换旧 Pod，不以进程重启制造依赖风暴。

首次进入 READY 后，`/ready`（kubelet/EndpointSlice 成员资格）对 NameServer/Broker 等共享依赖的短暂失败采用 fail-open；只有 listener 关闭、核心 executor 终止、runtime fatal 等 Pod 本地不可恢复故障，或显式 lifecycle 转换，才让 `/ready` 失败。依赖健康只反映在 `/ready-for-traffic`（provider health check），两个端点的谓词见 §3.2。

**依赖故障处理（复审 P1-3）：** 依赖失败时可以先通过 provider health 停止新连接，但**不得**在 READY 状态复用 `Server.shutdown()` 做「可重复 GOAWAY 后恢复」——`Server.shutdown()` 不可逆（见 §1.1）。可选策略只有三条，必须在实施前选定：

1. 在 provider 已证明的连接保留窗口内等待依赖恢复，受 `D_dependency_wait` 约束（见 §3.6）；
2. 对可证明的 Pod-local 持续故障进入**不可逆** drain，由 Pod replacement 恢复；
3. 另立完整的 listener rebuild 状态机与客户端迁移设计（超出本期范围）。

ACK 开启 connection drain 且 unhealthy 已启动倒计时时，恢复等待必须满足 `D_dependency_wait <= D_provider_keep - D_app_hard - D_skew - D_safety`；当前 ACK 30 秒不支持任意时长恢复等待，超时后必须执行预先定义的不可逆替换/客户端迁移策略，或改 provider 配置并重新实测。

**共享依赖故障的 fleet 行为：** 所有 Pod 同时 503 后再同步 GOAWAY 会形成 fleet reconnect storm。NLB all-target-unhealthy fail-open 仍会把连接路由回同一批 unhealthy targets，甚至不保证停止新连接，因此**不能**用它替代 quorum/fleet-aware 协调。本期不实现 fleet 协调，故 `/ready-for-traffic` 的依赖谓词必须配可调阈值，并把「全 fleet 同时失败」列为已知缺口。

### 3.2 管理接口

`enableProxyAdminServer=true` 时新增监听 `0.0.0.0:8082` 的内置管理服务，供 kubelet 和 provider health checker 直接访问 Pod IP；业务 Service 不暴露该端口：

- `GET /started`：本地组件初始化完成后返回 200；startupProbe 使用它，不等待外部 warmup。
- `GET /live`：仅不可恢复故障或 STOPPED 时失败；排空期间保持 200。
- `GET /ready`：lifecycle 允许成员资格、业务 listener 已绑定、无 fatal 时返回 200；共享依赖故障 fail-open。kubelet readiness/EndpointSlice 使用。
- `GET /ready-for-traffic`：startup/warmup barrier 已完成、业务 listener 已绑定、**依赖健康**、无 fatal 时返回 200。provider health check 使用。
- `GET /state`：仅 loopback 返回详细状态、原因、固定 cutoffs、各协议连接数、最后建连时间、send inflight、pending write/open RPC 和 forced 标志。
- `POST /drain`：仅允许 loopback socket peer；忽略 `Forwarded`/`X-Forwarded-For`。只创建或复用一次 `DrainRun`，**立即返回 `202 + runId`**，不阻塞。
- `GET /drain/{runId}`：轮询 drain 结果。

**所有 handler 必须短时、非阻塞（复审 P1-4）。** 原 `?wait=true&waitTimeoutSeconds=N` 的阻塞契约删除：admin server 只有固定两个线程，两个并发或重入 waiter 就会让 `/live`、`/ready` 无线程可用，导致 Pod 在 drain 中被 kubelet 杀死。JDK `HttpServer` 只有 server 级 `setExecutor()`，无法按 `HttpContext` 分配 executor，因此「保留一个线程」不可实现。

必须落实：有界队列、请求并发上限与单请求超时；在 `POST /drain` flood 下验证 `/live` 最大延迟作为门禁；admin executor 自身服从 `StopDeadline`。若后续确实要保留 blocking waiter，则 health 必须使用**另一个** `HttpServer`/listener 或显式 dispatcher 与独立执行资源。

`/ready-for-traffic` 在计划内 drain 时是否继续返回 200，只能在三个条件同时成立时采用：EndpointSlice/provider deregistration 已停止新连接；provider 被证明不会因此终止既有连接（见 §4.2 属性锁定与抓包证据）；STARTING 与 STOPPED 阶段绝不提前或继续报 200。

admin 开启但 lifecycle 关闭时，三个 health GET 使用兼容就绪语义，`/state` 明确显示 `lifecycleEnabled=false`，POST drain 返回 409；这只服务首次 bootstrap，不构成严格能力。镜像提供 `mqproxyctl drain --wait --timeout 480s`，通过 loopback 调用管理接口；`--wait` 由 **CLI 侧轮询** `GET /drain/{runId}` 实现，服务端 handler 始终不阻塞（见本节末），`--timeout` 只约束 CLI 自身等待，不创建也不延长 coordinator deadline。SIGTERM shutdown hook 加入或启动同一个 `DrainRun` 作为绕过 PreStop 时的幂等兜底，然后进入独立的 `StopRun` 并同步 `join()` 到 STOPPED；HTTP drain future 完成时 admin 仍存活。NetworkPolicy 只作第二层保护，远程 POST 必须在应用层拒绝。

### 3.3 连接租约

默认配置：

- 名义连接寿命：300 秒。
- gRPC：`maxConnectionAge=300s`，使用 gRPC 内建 ±10% 抖动，`maxConnectionAgeGrace=30s`。
- Remoting：Channel 建立时分配 270–330 秒租约。
- gRPC 的连接租约和全局 `Server.shutdown()` 都复用 grpc-java/Netty 内建标准双 GOAWAY；Proxy 不直接访问 package-private handler、不手写 HTTP/2 帧，也不发送 RocketMQ Telemetry Reconnect 命令。
- Remoting `> V5_3_1` 的**非 oneway** 请求在租约后的首个 acknowledged request 进入业务处理前返回 GO_AWAY；客户端在 transport 层重连同一逻辑地址并重放**一次**。第二次收到 GO_AWAY 会失败，因此重放只能缓冲单次迁移，**不能承载正确性**。oneway 与旧版本客户端不满足该路径，见 §1.2。
- 空闲过期 Remoting Channel 在无 inflight 时关闭；旧版客户端继续服务到排空 deadline，但记入 legacy 指标。
- 正常扩容不主动驱逐 Pod，只依靠租约让全部活跃连接在约 5.5 分钟内至少重新建连一次。
- **steady-state lease 是容量/SLO 取舍，需压测定标（复审 P1-1）。** 300 秒 `maxConnectionAge` 带来 TLS/HTTP/2/CPU 与连接重建成本，同时承担扩容重平衡目标；关闭、延长或保留必须由压测决定，不得仅凭静态判断改成 1800 秒。压测至少输出：每 Pod **连接数 CV**（不用请求 QPS CV，见 §5.2）、TLS handshake 与 CPU/GC、连接建立失败与 reconnect 峰值、扩容后达到目标连接分布的 P95/P99 时间、以及与事务消息策略（§9.4）的联合影响。
- **不设计不存在的 gRPC 分批 GOAWAY（复审 R4）。** grpc-java 公共 API 无按连接分片发送 GOAWAY 的能力；reconnect storm 通过 Pod 串行 drain、容量 headroom、客户端真实 backoff 与压测控制。Remoting 可按 child channel 批处理，但必须验证遍历成本、客户端回流到同一 Pod 的概率与第二次 GO_AWAY 行为。
- drain 的 LB cutoff 后若仍有新 transport 到达，视为 provider 摘流违约且不得进入业务。gRPC transport 通过 Attributes 标记为 late，interceptor 拒绝其全部新 send RPC，最终由 max-age 或 migrationCutoff 的全局 shutdown 终止；公开 `ServerTransportFilter` 没有单 transport close handle，不虚构立即关闭能力。Remoting 5.3.2+ 返回 GO_AWAY，legacy 关闭连接并计入降级指标；晚到连接不得延长本次 drain。

### 3.4 原子准入、双终态与 transport 屏障

新增 `SendDrainGate`，用一个 packed `AtomicLong` 的关闭位和 accepted 计数实现 `tryAcquire()`/`closeAdmission()` CAS 线性化；permit 自身用 CAS once-only 完成。禁止用“先读状态、再加 LongAdder”或轮询两次为零替代原子门闩。

必须始终满足以下不变量：

1. `closeAdmission()` 返回后绝不再产生新 permit；关闭前成功获取的 permit 已计入 accepted。
2. 每个 acknowledged send 独立维护 `backendTerminal` 与 `protocolTerminal`，仅两者都为 true 时才释放 permit。
3. `DRAINED := admissionClosed && acceptedSends==0 && remotingPendingWrites==0 && grpcOpenSendRpcs==0 && grpcOpenDrainableCalls==0 && protocolIntakeFrozen && transportsTerminated`。
4. cancel、channel/transport close 只能标记 protocol terminal，不能代替尚未完成的 Broker Future；transport 级 callback 永远不能批量释放 send permit。
5. 服务端只证明 Remoting 本地 `ChannelFuture` 终态或 gRPC stream 终态，不声称证明客户端已收到响应；远端收包证明需要应用层 ACK，超出本方案范围。

permit 的 backend 状态使用 `NOT_STARTED -> STARTED -> TERMINAL` 或 `NOT_STARTED -> SKIPPED` 单调 CAS。只有确定 handler 未 dispatch Broker 时才能写 SKIPPED；一旦 STARTED，cancel/close 必须等待真实 Future terminal。

#### 唯一关闭顺序（复审 P0-1、Step 2）

全文只有一套关闭顺序，即 §1.1 的「先迁移 → 到达经验证的 no-new-work 边界 → 才关闭 admission」。每个协议独立走以下四个可观测里程碑，唯一的变量是第 2 步的谓词，由 §7 Step 1 的 spike 填入：

| 里程碑 | 语义 | 作用域 |
|---|---|---|
| `migration_started` | 向该协议存量连接发起迁移：gRPC 一次性 `initiateServerDrain()` 触发内建双 GOAWAY；Remoting 按 `clientVersion × invocation mode` 返回 GO_AWAY 或关闭 idle channel | 单协议 |
| `no_new_work_reached` | 该协议已不再向业务分发新请求的**经验证**条件 + `freezeRequestIntake()` 类的 intake 冻结完成 | 单协议 |
| `admission_closed` | 共享 `SendDrainGate.closeAdmission()`，由**最后**到达 `no_new_work_reached` 的协议触发，全程只调用一次 | 全局，一次 |
| `accepted_inflight_zero` | 已 accepted 的 send 双终态归零 | 单协议观测、共享 gate 判零 |

因此 QUIESCING/MIGRATING 全程 gate 保持开放；`closeAdmission()` 不再是「进入 DRAINING 的第一个动作」，而是两个协议都已证明 no-new-work 之后的收口动作。协议迁移彼此独立并发，不得让一个协议等待另一个协议的 gate 状态才开始迁移。

第 2 步的谓词是**配置化 predicate**，不是文档假设。spike 只能得出两种结论，二者都落到同一顺序，不产生第二套流程：

- 若双 GOAWAY（或 Remoting 的 GO_AWAY + intake freeze）本身即构成 no-new-work 边界，`no_new_work_reached` 直接由该证据满足；
- 若之后仍可能有新 stream/请求进入业务（strict client 竞态），则 predicate 退化为「migration 已发起 + 有界 quiet 窗口内未观察到新业务分发」，并且**必须**按 §1.1 四选一显式放宽目标并记录：此时不能再声称「不主动拒绝仍可能合法到达的请求」。quiet 窗口长度是配置项，受同一 effective deadline 约束，超时即按 late 处理并计入指标，不得无界等待。

同步校验失败、未 dispatch、executor reject 等没有 Broker Future 的路径必须显式写入 synthetic backend terminal，不能靠 finally 猜测完成。

**Remoting 实现：**

- `processRequestCommand` 固定顺序为 processor lookup → lifecycle/request 分类 → lease/gate → flow-control reject → 构造/提交 task。在 `ExecutorService.submit` 之前调用可选 `RemotingRequestLifecycleListener.beforeEnqueue`；默认实现为 NOOP，Broker/NameServer 行为不变。
- 返回的非 wire admission context 以 `RequestTaskContext` 直接随扩展后的 `RequestTask` 传递；任务执行时通过严格清理的 scoped context 交给 `AbstractRemotingActivity`，由其立即保存到 `ProxyContext` 供异步 callback 使用。禁止使用 channel+opaque side map。
- lease GO_AWAY、`rejectRequest`、submit reject、`RequestTask.stopRun`、`shutdownNow` 回收和 channel-before-dispatch 都必须终止同一个 context；未进 Broker 的路径标记 synthetic backend terminal，并跟踪拒绝响应写回或主动关连接。
- `MessagingProcessor.request` Future 完成标记 backend terminal；统一 tracked writer 在 `writeAndFlush` 前递增 `remotingPendingWrites`，在 `ChannelFuture` 成功/失败、同步抛错或 channel close 时标记 protocol terminal 并递减。`isWritable=false` 只是背压水位，不再静默丢响应；channel active 时仍写并观察 Future，失败时关闭连接唤醒客户端重试。
- DRAINING 后新 send 永不进入 Broker：5.3.2+ 返回 GO_AWAY；legacy 返回可重试错误后关闭并记为 degraded。事件循环 barrier 等待此前 decode/dispatch/write task 入队，再关闭 acceptor、设置现有 channel `autoRead=false`，并设置 drain-frozen attribute，禁止 `channelWritabilityChanged` 再次开启 auto-read，杜绝 drain 判零后出现晚到 `RequestTask`。
- oneway 不进入严格 gate；已开始的 `requestOneway` 只做 best-effort 独立计数，并在方法返回后完成。

**gRPC 实现：**

- gRPC 的 tracer 先于 interceptor 创建，不能让 interceptor 单向“传 token 给 tracer”。`ServerStreamTracer.Factory` 先为 `MessagingService/SendMessage` 创建 per-stream mutable holder，`filterContext()` 把 holder 放入 `io.grpc.Context`；`ServerInterceptor` 从该 Context 取 holder，在 handler 前获取 permit 并 CAS 绑定。业务 Future 与 `streamClosed()` 只操作这个 holder，禁止各自独立计数。
- holder 从 tracer 创建起计入 `grpcOpenSendRpcs`，所以 gate 拒绝和同步失败的 send RPC 也一直统计到 stream terminal；重复绑定、无 holder 或 method 不匹配必须 fail loudly 并进入可观测拒绝路径。
- `GrpcMessagingApplication.sendMessage` 必须先 CAS `NOT_STARTED -> STARTED` 成功才可调用 Broker，`CompletableFuture` 完成后置 TERMINAL；若 ACL/同步校验/cancel 在 dispatch 前结束 RPC，wrapped call/listener 或 `streamClosed` CAS 为 SKIPPED，后到的业务入口因 STARTED CAS 失败而禁止 dispatch。`ServerStreamTracer.streamClosed` 是唯一 canonical protocol terminal；`ServerCall.close` 只记录 response-close intent，cancel/deadline 只记录原因，均不得把已 STARTED 的 backend 提前终止。
- `ServerTransportFilter.transportReady` 只把建连时间和 late 标记写进 transport Attributes，interceptor 从 `ServerCall.getAttributes()` 拒绝 late transport 的 send；不得把 transport filter 当成可关闭 channel 的 API，也不得由 `transportTerminated` 直接释放 holder/permit。
- 原始 `io.grpc.Server`、TLS reload listener 和 builder 创建的 boss/worker EventLoopGroup 只能由 `GrpcServer` 持有；`GrpcDrainAdapter` 不得持有或直接操作这些对象，只能调用 `GrpcServer` 的阶段化生命周期接口。严格路径禁止再调用现有固定 `grpcShutdownTimeSeconds` 的一体式 `shutdown()`。
- **顺序遵循本节「唯一关闭顺序」表：** 进入 DRAINING 时先对 gRPC 调用一次 `GrpcServer.initiateServerDrain()`（`migration_started`），到达 spike 验证的 `no_new_work_reached` 后才由最后一个协议触发共享 `closeAdmission()`（`admission_closed`），不得在迁移前关 gate。`initiateServerDrain()` 只 CAS 幂等调用非阻塞的 `Server.shutdown()` 并立即返回。grpc-java 1.53.0 随后按连接发送 `GOAWAY(lastStreamId=MAX, NO_ERROR) -> PING -> PING_ACK 或 10 秒兜底 -> GOAWAY(lastStreamCreated, NO_ERROR)`，Proxy 不重复实现 GOAWAY。`no_new_work_reached` 的 predicate 由 §7 Step 1 spike 填入：若双 GOAWAY 即构成边界则直接满足，否则退化为有界 quiet 窗口并按 §1.1 四选一放宽目标。
- 新增 `GrpcActiveCallRegistry` 和 `GrpcActiveCallInterceptor`，在 `next.startCall` 前登记所有非 unary RPC，因此尚未发送第一条 SETTINGS 的 Telemetry 也已被覆盖。registry 保存包装后的 `GrpcActiveCall`，包装 call 的 `sendHeaders/sendMessage/close` 使用同一互斥区和 once-only close-intent；wrapped listener 只在 `onComplete/onCancel` 时发布 canonical terminal、移除 registry 并递减 `grpcOpenDrainableCalls`。`ServerCall.close()` 返回、应用 observer 的 `onError/onCompleted` 或发出 GOAWAY 都不能直接把 call 计为 terminal。
- accepted send 的 backend/protocol 双终态归零后，`GrpcActiveCallRegistry.closeAll(GrpcDrainStatusPolicy)` 主动结束非 unary RPC：Telemetry 使用 `Status.OK` 正常 completion，使 5.0.7/5.2.1 走无错误日志的 1 秒 observer renewal；ReceiveMessage/PullMessage 及未来未知 streaming RPC 默认使用 `Status.UNAVAILABLE.withDescription("[PROXY_DRAINING] reconnect")`，防止把被截断的业务流伪装成成功。两类 close 都单独计数、不能混入 logical send 错误；close 与正常 response/cancel 的竞态必须串行且幂等。Proxy 组件测试锁定 wire status，客户端矩阵锁定两个正式版本的真实 renewal。随后才用同一个 effective deadline 调用 `GrpcServer.awaitServerTermination(remaining)`；返回 `false` 或中断必须增加 forced 指标、调用一次 `forceServerShutdown()`，并返回 forced `DrainResult`。
- `ServerCall.close`/`streamClosed` 不是 socket flush 或客户端收包证明。最后一个 send stream terminal 后仍保留有界 transport grace，并以 `Server.awaitTermination` 与客户端侧零观测错误共同验收。

正常协议排空接受同一个 `DrainSession` 的剩余时间；一旦 direct TERM 发布 StopRun，尚未完成的协议阶段改用两者的最短 effective deadline。进入 STOPPING 后，全部执行器和嵌套 owner 只接受同一个 `StopRun.stopDeadline`。执行器统一 `shutdown -> awaitTermination(remaining) -> shutdownNow`，任何 `false`、中断或丢弃队列都必须可观测。

### 3.5 规范化配置契约

所有配置均为启动时只读；非法组合由 Proxy 启动校验和 Helm schema 同时拒绝。admin 与 lifecycle 分离，以便旧镜像 bootstrap；生命周期总开关关闭时不启用连接租约/准入门，并继续使用旧 probe/PreStop。严格生产 profile 必须同时开启 admin、lifecycle 和全部协议子能力。

| Java key | Helm key | 默认值 | 约束与失败策略 |
|---|---|---:|---|
| `enableProxyAdminServer` | `proxy.lifecycle.admin.enabled` | `false` | 可独立于 lifecycle 开启；首次迁移阶段 A 使用。 |
| `enableProxyGracefulLifecycle` | `proxy.lifecycle.enabled` | `false` | 仅 Cluster mode；严格 profile 必须为 true。 |
| `enableProxySendDrain` | `proxy.lifecycle.sendDrainEnabled` | `false` | 依赖 lifecycle；显式开启 gRPC send tracer/interceptor、bind pipeline 与 activity decorator。严格生产 profile 必须为 true；关闭时仍保留连接迁移和 active-call drain，但不提供 accepted-send 双终态排空保证。 |
| `proxyAdminBindAddress` | `proxy.lifecycle.admin.bindAddress` | `0.0.0.0` | health 可被 kubelet/NLB 访问；变更接口仍仅接受真实 loopback peer。 |
| `proxyAdminPort` | `proxy.lifecycle.admin.port` | `8082` | 1024–65535，且不得与业务/metrics 端口冲突。 |
| `proxyGrpcConnectionLeaseEnabled` | `proxy.lifecycle.grpcLease.enabled` | `true` | 只在总开关开启时生效；严格 profile 不允许关闭。 |
| `proxyRemotingConnectionLeaseEnabled` | `proxy.lifecycle.remotingLease.enabled` | `true` | 只在总开关开启时生效；严格 profile 不允许关闭。 |
| `proxyConnectionLeaseSeconds` | `proxy.lifecycle.connectionLeaseSeconds` | `300` | 60–3600；gRPC 名义 age，Remoting jitter 基数。 |
| `proxyConnectionLeaseGraceSeconds` | `proxy.lifecycle.connectionLeaseGraceSeconds` | `30` | 1–lease；用于 gRPC age grace 和迁移预算。 |
| `proxyRemotingLeaseJitterRatio` | `proxy.lifecycle.remotingLeaseJitterRatio` | `0.10` | 0–0.50；默认产生 270–330 秒租约。 |
| `proxyLbDetachQuietSeconds` | `proxy.lifecycle.lbDetachQuietSeconds` | `20` | 仅作观测信号，必须小于 detach timeout，不能重置 deadline。 |
| `proxyLbDetachTimeoutSeconds` | `proxy.lifecycle.lbDetachTimeoutSeconds` | `60` | 到点后拒绝新 transport；provider 违约使验收失败。 |
| `proxySendDrainTimeoutSeconds` | `proxy.lifecycle.sendDrainTimeoutSeconds` | `30` | 1–120；只用于 fixed migration cutoff 后的 send drain。 |
| `proxyWarmupTopics` | `proxy.lifecycle.warmupTopics` | `[]` | 兼容配置可空；严格生产至少配置每个关键路由域的代表 topic。 |
| `proxyWarmupTimeoutSeconds` | `proxy.lifecycle.warmupTimeoutSeconds` | `60` | 1–600；超时保持 NotReady 并退避重试，不退出进程。 |
| `proxyPreStopWaitSeconds` | `proxy.lifecycle.preStopWaitSeconds` | `480` | 1–3600；必须大于等于 drain hard deadline，作为 PreStop 上限及 TERM stop deadline 的绝对封顶基准。 |
| `proxyJvmShutdownTimeoutSeconds` | `proxy.lifecycle.jvmShutdownTimeoutSeconds` | `30` | 5–120；超时进入 forced close 并阻断验收。 |
| `proxyLegacyRemotingDrainPolicy` | `proxy.lifecycle.legacyRemotingPolicy` | `SERVE_UNTIL_CUTOFF` | 只允许 `SERVE_UNTIL_CUTOFF`；legacy 永远不进入严格等级。 |
| `proxyDependencyFailureThreshold` | `proxy.lifecycle.dependencyFailureThreshold` | `3` | `/ready-for-traffic` fail-close 的连续失败次数；`/ready` 不受影响（§3.1）。 |
| `proxyDependencyWaitSeconds` | `proxy.lifecycle.dependencyWaitSeconds` | `0` | `D_dependency_wait`；`0` 表示不等待、直接按选定策略处置。必须满足 §3.6 的依赖不等式。 |
| `proxyReadyForTrafficDuringDrain` | `proxy.lifecycle.readyForTrafficDuringDrain` | `false` | 保守默认：drain 期间 `/ready-for-traffic` 返回 503。仅在 §4.2 spike 证明 provider 不会因 unhealthy 终止既有连接后才允许开启（§8.4）。 |
| `proxyAdminMaxConcurrentRequests` | `proxy.lifecycle.admin.maxConcurrentRequests` | `8` | admin 有界并发；超出返回 503，保证 health handler 不被饿死（§10.1）。 |

Helm 还必须校验：lifecycle=true 蕴含 admin=true；`remotingAccessAddr` 是稳定 Service/NLB DNS 和端口；PreStop、Pod grace、NLB deregistration、`minReadySeconds` 满足下一节的命名预算不等式；生命周期开启时不能渲染 TCP probe 或 `mqshutdown` PreStop；health probe 必须指向 admin port 的 HTTP path 而非业务端口 `tcpSocket`。

### 3.6 单一时钟与默认预算

首次 `beginDrain` 以 `System.nanoTime()` 创建不可变 `DrainSession`，定义 `T0` 和固定 cutoffs；墙钟只用于日志。Pod deletion 通常略早于 T0，因此 PreStop 与 Pod grace 额外保留安全余量。

```text
maxLease          = ceil(300s * (1 + 0.10)) = 330s
lbCutoff          = T0 + 60s
migrationCutoff   = T0 + 60s + 330s + 30s = T0 + 420s
drainHardDeadline = T0 + 420s + 30s = T0 + 450s
preStopWait       = T0 + 480s
podGrace          = deletion start + 540s
rollout step      >= 600s
```

1. T0：进入 QUIESCING，readiness 失败；同时触发 EndpointSlice/NLB 摘流。
2. 持续观测业务 transport 的最后建连时间，健康检查/管理连接不计入。20 秒 quiet 只用于诊断；无论是否提前 quiet，都持续守到固定 lbCutoff。
3. lbCutoff 后标记并拒绝任何晚到 transport 的业务：Remoting 可关闭 Channel，gRPC 只拒绝 send 并等 max-age/全局 shutdown。cutoff 前最后一条连接的 lease+grace 必须在 migrationCutoff 前结束；任何连接都不能延长 session。
4. 若 lbCutoff 后业务连接已为零，可提前进入 DRAINING；否则在 migrationCutoff 关闭 admission gate、冻结 intake，并等待双终态至 hard deadline。
5. hard deadline 尚未满足 DRAINED 条件则进入 FORCE_DRAINING，强制关闭并让本轮验收失败；PreStop waiter 最迟 480 秒返回非零，不能把强制路径报告为成功。
6. HTTP/PreStop 只等待 drain future，在 DRAINED 或 FORCE_DRAINING 返回；TERM 首次进入 STOPPING 时创建独立 stop future，并计算 `stopDeadline=min(now+30s, T0+480s+30s)`。若 TERM 绕过/打断 PreStop，不能再等待 60 秒 lbCutoff 或 420 秒 migrationCutoff：必须立即关 gate、freeze 两协议，所有剩余 drain/force/final close 共用该 stop deadline。hook 必须同步 `join()`，最后保留约 30 秒 kubelet 余量；`terminationGracePeriodSeconds=540`。

正常排空 await 使用 `min(phaseCutoff, hardDeadline) - now`；StopRun 出现后再与 stop deadline 取最短。最终关闭 await 使用同一 `stopDeadline-now`，禁止阶段或子组件开始时重新获得完整预算。重复 drain 返回同一 `DrainRun`，重复 TERM 返回同一 `StopRun`；更短的外部停止预算只能收紧，任何调用都不能延长。单个 send 保持原 request deadline，drain deadline不得延长 Broker 超时。

NLB 450 秒 deregistration/connection-drain 与 Proxy 时钟并行，从 readiness/target 摘除开始计算，绝不能再串行加到 540 秒之后。若 provider 实测最后新连接 p999 超过 60 秒，或健康检查无法直达 Pod 8082，严格 profile 必须阻断上线并整体重算全部预算，不能压缩 send drain。

#### 命名预算与安全不等式（复审 P0-2、R2）

上面的具体秒数是**派生结果**，不是配置源。必须先定义以下命名预算，并由**同一配置源**渲染 Helm、应用、PreStop 和 rollout supervisor：

| 名称 | 含义 |
|---|---|
| `D_app_hard` | 应用 drain hard deadline |
| `D_provider_keep` | provider 保留既有连接的最短时间 |
| `D_prestop` | PreStop 最长等待 |
| `D_process_stop` | TERM 后资源停止预算 |
| `D_dependency_wait` | dependency unhealthy 后允许原地恢复的最长等待（见 §3.1） |
| `D_skew` | EndpointSlice、controller、health check 与观测传播裕量 |
| `D_safety` | 生产抖动裕量 |

至少满足：

```text
D_provider_keep >= D_app_hard + D_skew + D_safety

# dependency unhealthy 已启动 provider drain 时
D_dependency_wait + D_app_hard + D_skew + D_safety <= D_provider_keep

terminationGracePeriodSeconds >= D_prestop + D_process_stop + D_safety
```

**Kubernetes 阶段不可简单串行相加：** Pod grace period 在执行 PreStop 前已开始倒计时，EndpointSlice 的 terminating/ready 变化与 PreStop 也可能并行传播。`D_prestop` 若等待应用 drain 完成，必须小于 Pod grace。应用、PreStop 与 supervisor 必须引用同一份派生值，**禁止分别复制默认常量**（`proxyLbDetachTimeoutSeconds` 已是配置项，缺口是各处复制默认值）。

启动时交叉校验并对不满足不等式的配置 **fail-fast**：provider keep timeout、Broker heartbeat/registration timeout、send drain deadline、PreStop 与 Pod grace、exporter/worker stop budget。

### 3.7 可观测性

通过现有 Proxy metrics exporter 输出低基数指标：

- `proxy_lifecycle_state{state}`
- `proxy_connections{protocol,capability}`
- `proxy_new_connections_after_quiesce_total{protocol}`
- `proxy_new_connections_after_lb_cutoff_total{protocol}`
- `proxy_connection_lease_expired_total{protocol,reason}`
- `proxy_inflight_sends{protocol,terminal_state}`
- `proxy_pending_response_writes{protocol="remoting"}`
- `proxy_open_send_rpcs{protocol="grpc"}`
- `proxy_open_grpc_drainable_calls{rpc_type="telemetry|server_streaming|client_streaming|bidi_streaming"}`
- `proxy_grpc_drainable_call_close_total{rpc_type,close_mode="completed|retryable_error",outcome="requested|terminal|failed"}`
- `proxy_drain_duration_seconds`
- `proxy_drain_deadline_exceeded_total{phase}`
- `proxy_transport_termination_failed_total{protocol}`
- `proxy_warmup_status{contributor}`
- `proxy_forced_close_total{protocol,reason}`

日志按 drain ID 记录状态迁移、固定 cutoffs、初始/最终连接数、legacy 客户端数和 force 原因；禁止使用 client ID 等高基数标签。默认 PrometheusRule 与升级 gate 将 `late connection after cutoff`、deadline exceeded、transport termination failure、forced close 的任一增量视为失败；warmup 超时阻止 rollout 继续。

**补齐触发源、阶段耗时与 cutoff 快照（复审 R3）。** 至少区分：触发源 `PRESTOP` / `SIGTERM_FALLBACK` / `ADMIN` / `DEPENDENCY_FAILURE`；每一阶段的开始、完成、超时；effective deadline 与剩余时间快照；accepted/rejected/migrated/forced 数量；首个失败资源与最终 terminal。协议级还需记录 `migration_started`、`no_new_work_reached`、`admission_closed`、`accepted_inflight_zero`（§1.1 的 no-new-work 边界证据）。

#### 最终 metrics 不能作为 rollout 控制的唯一来源（复审 P1-6）

`ProxyMetricsManager.shutdown()` 当前调用异步 `forceFlush()`/`shutdown()` 后不等待结果；Prometheus 也不存在「关闭 metrics 前一定被最终 scrape」的屏障。loopback admin `/state` 在 Pod 退出后不可达、内存结果也会消失，因此**都不能**作为 supervisor 的主判据。

必须落实：

- OTLP/LOG exporter 在 remaining deadline 内等待 `CompletableResultCode`；
- PROM 仍暴露 phase/forced 指标，但 rollout supervisor 不得只依赖最后一次 scrape；
- 选定一个**可达且持久**的结果交接：结构化 termination message 并由串行 supervisor 确认、Pod Condition/CRD，或删除前通过受认证的 Pod-IP endpoint/exec 读取；
- rollout pause 以该已确认的 drain result 为主、指标为辅，并定义交接写入/读取失败时 **fail-closed**；
- 明确 missing series、stale series、query timeout 与 controller restart 时的 fail-closed 行为。

## 4. Helm 与容器落地

### 4.1 主 Chart：严格生产基线

- 保持 `maxSurge: 1`、`maxUnavailable: 0`，设置 `minReadySeconds: 600`、`terminationGracePeriodSeconds: 540` 和至少 1800 秒的 `progressDeadlineSeconds`。600 秒不是 warmup 延迟，而是必须大于 540 秒终止上界的 rollout 串行化护栏：第二个新 Pod 计入 Available 前，上一个旧 Pod 必须已经退出。
- startup/readiness/liveness 分别使用 8082 `/started`、`/ready`、`/live`；NLB health check 单独使用 `/ready-for-traffic`（见 §4.2）。PreStop 执行 `mqproxyctl drain --wait --timeout 480s`（CLI 侧轮询），删除 `mqshutdown proxy || true`。
- Helm command、`distribution/bin/mqproxy`、stock/docker `runserver.sh` 全部以 `exec` 传递到 Java；Docker 镜像安装 `curl`。
- **容量门禁使用不等式，不使用魔法副本数（复审 R5）。** 不写死「最少 3/4 replicas」，而要求在一个 Pod draining 加所选 failure-domain 损失后满足：

  ```text
  remaining_ready_capacity >= peak_required_capacity * safety_factor
  ```

  同时约束 `maxUnavailable=0`、surge 是否落在独立 failure domain、Pod headroom 与连接重建峰值。Service 不得启用 ClientIP affinity 或 `publishNotReadyAddresses`。
- Remoting 必须设置稳定 `remotingAccessAddr`；外部 Remoting 场景为 NLB 增加 8080 listener，禁止发布 Pod IP。
- PDB 改为 values 驱动并推荐 `maxUnavailable: 1`；明确 PDB 只约束 Eviction。
- 修复 Proxy NetworkPolicy selector，8082 只允许节点/kubelet 和 provider health checker；应用层仍依据真实 socket peer 拒绝非 loopback 的 `/state` 与 POST drain。
- 增加 Proxy 跨节点/AZ spread，并验证 `maxSurge=1` 有调度余量。
- **固定 replicas，删除 HPA freeze/unfreeze 状态机（复审 P1-8）。** 生产明确使用固定副本数，因此不为 Proxy 增加 HPA schema、不做 prepare revision / frozen overlay / restore 三段事务。改为：Deployment 始终渲染 `spec.replicas`；rollout supervisor **启动前检查目标 workload 没有 live HPA**，检测到则 fail（防止将来有人启用后静默破坏串行 drain）。这一简化削减的正是崩溃恢复路径最多、最难测试的部分。
- 人工缩容由升级脚本逐级减 1，并等待上一 Pod DRAINED/删除完成；直接从 5 缩到 3 不属于严格流程。
- 升级脚本持续断言最多一个旧 Pod 处于 Terminating；若 Kubernetes 版本或控制器行为突破该约束，立即 pause Deployment。本约束必须通过真实集群测试，不能只凭 `maxUnavailable=0` 推断。

### 4.2 NLB 时钟与 health-port 证据门

- AWS 与 ACK NLB 使用 Pod IP 8082 `/ready-for-traffic` HTTP health check，但不创建面向业务的 8082 listener。实现前必须在两个真实 provider 做 spike，证明 target group 可把 health-check port 指向非业务 target port；任一 provider 不支持时，该 profile 在替代拓扑确定前不得宣称严格。
- **当前 Helm 现状必须一并修改：** startup/readiness/liveness 都是业务 gRPC 端口的 `tcpSocket`，AWS 生产值是 TCP 8081 health check，ACK 未显式配置 HTTP health check。因此必须暴露命名 admin health port、把 kubelet probes 改为对应 HTTP path、分别为 AWS/ACK 配置 health-check protocol/port/path，并在模板测试中断言生成值——否则会出现「Java endpoint 已实现但流量仍由 TCP 探针控制」。
- 默认 target deregistration/connection drain 为 450 秒，不得在 Proxy 响应排空完成前强制断开现有连接。
- provider values 必须锁定 target type、health interval/threshold、cross-zone、externalTrafficPolicy 和连接终止属性。

**Provider 必须分开建模（复审 P0-2）。** 不得共用一句「关闭 unhealthy termination 后连接一定安全」的结论；每个生产 provider 需独立配置、实测 P95 与 packet-level 证据：

| Provider/路径 | 需锁定或验证的行为 | 当前结论 |
|---|---|---|
| AWS NLB：target deregistration | `deregistration_delay.connection_termination.enabled=false`，验证 delay 结束后既有连接行为 | 需显式配置并抓包验证 |
| AWS NLB：target unhealthy | `target_health_state.unhealthy.connection_termination.enabled=false`，避免 unhealthy 时主动终止既有连接 | 需显式配置（**AWS 默认为 `true`**） |
| AWS NLB：all targets unhealthy | 会 fail-open 到所有 registered targets | **不能**替代应用层 fleet 协调 |
| ACK NLB：connection drain | timeout 到期后会主动关闭既有连接 | 当前 30 秒**短于**计划迁移时钟，不安全 |

- 真实环境测量“readiness 失败后最后一个新连接到达”的 p99/p999，并以 p999 校准 60 秒摘流预算。
- NetworkPolicy 开启后必须同时验证 kubelet 与 NLB health checker 可访问 8082，而业务源不能调用管理操作；不得信任源地址转发 header。

官方依据：[Kubernetes Pod and Endpoint termination flow](https://kubernetes.io/docs/tutorials/services/pods-and-endpoint-termination-flow/)、[AWS NLB target group attributes](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/edit-target-group-attributes.html)、[AWS NLB fail-open behavior](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-troubleshooting.html)、[ACK NLB connection draining](https://www.alibabacloud.com/help/en/slb/network-load-balancer/user-guide/create-and-manage-a-server-group)、[AWS Load Balancer Controller Service annotations](https://kubernetes-sigs.github.io/aws-load-balancer-controller/latest/guide/service/annotations/)、[ACK NLB annotations](https://www.alibabacloud.com/help/en/slb/network-load-balancer/use-cases/configure-nlb-instances-by-using-annotations)。

### 4.3 Standalone Chart

Standalone 接入相同管理端点、HTTP probes、PreStop、exec 链、显式滚动策略和终止预算，但保留默认单副本并标记为降级环境。它不承诺驱逐、缩容或异常重启对 send 无感。

### 4.4 升级工具

- `upgrade-prod.py` 增加 lint/template/schema、容量不等式（§4.1）/PDB/预算、surge 调度余量、当前 Pod READY、无进行中 drain、previous revision 兼容性和 provider health-port 证据的 preflight；并**断言目标 workload 无 live HPA**，检测到即 fail。
- 严格模式禁止无监督的 `helm upgrade --atomic`：自动 rollback 会在异常时再触发反向 rollout。脚本以 `helm upgrade --wait` 启动受监控 rollout，持续观察 Pod drain、NLB 和 SLO；异常时立即 pause Deployment、停止后续删除并保留 READY Pod。
- timeout 至少为 `副本数 × minReadySeconds + terminationGrace + 启动余量`。受控 rollback 是独立子命令，使用相同的串行 drain gate；前一 revision 不具备生命周期时只能执行 bootstrap 级回退，不能声称严格。
- rollout 后检查所有 Pod READY、NLB targets healthy、forced/late-connection 指标无增量、旧 Pod 无 SIGKILL，并继续观察完整 send SLO 窗口。
- **rollout 判据来源（复审 P1-6）：** pause/继续的主判据必须是 §3.7 选定的可达且持久的 drain result 交接，指标仅为辅助；交接写入/读取失败、missing/stale series、query timeout 与 controller restart 一律 **fail-closed**。
- **forced 分级处理：** `forced && accepted_sends > 0`（有 acknowledged send 未拿到双终态，可能真丢）→ pause rollout；`forced && accepted_sends == 0`（send 已安全终止，仅流/transport 收尾慢）→ warn + 记录并继续。该策略必须由产品显式确认为阻断/人工确认/告警继续之一，不得由本计划代为决定；`proxy_forced_close_total{protocol,reason}` 需增加可区分两者的维度。
- 修复生产 PDB values 被模板硬编码覆盖、Proxy placement 空值覆盖全局配置和 NetworkPolicy selector 错误。
- 独立 canary 使用单独 namespace/release、Deployment selector、Service/NLB 和测试客户端入口，不加入主 Service；至少 3 副本，实际删除一个 canary Pod 验证 provider 摘流和三种严格客户端后才能进入 bootstrap。

## 5. 测试与验收

### 5.1 单元与组件测试

所有新增 Java 测试必须使用 `@DisplayName`，先写失败测试再实现：

- 生命周期状态单调性、STARTING 直达停止、重复 drain 复用同一 `DrainRun`、HTTP drain 不推进 STOPPING、重复 TERM 复用独立 `StopRun`、wait timeout 不延长 hard deadline、stop deadline 双上限、正常/forced 路径互斥和 once-only close。
- `/started`、`/live`、`/ready`、loopback-only `/state`/drain；伪造 forwarded header 无效；初次 warmup、warmup 超时恢复、READY 后共享依赖波动 fail-open 和本地 fatal fail-close。
- `SendDrainGate` 的 packed CAS：`closeAdmission` 与数千并发 `tryAcquire` 竞态后没有 late permit，backend/protocol 两种完成顺序和重复 callback 均不下溢。
- gRPC builder 的 age/grace；tracer holder 经 `filterContext()` 先注入、interceptor 后 CAS 绑定同一 permit；backend-first、stream-first、cancel-before-backend、deadline、重复 terminal；`streamClosed` 先于 transport termination 时不得发布 DRAINED；真实饱和 executor 必须证明 `execute(GrpcTask)` 的 reject 路径不会泄漏 permit。
- late gRPC transport 只通过 call Attributes 拒绝 send，并在 migrationCutoff 全局终止；测试禁止假设 `ServerTransportFilter` 能直接 close transport。
- `Server.shutdown()` 恰好一次且先于第二阶段等待；真实 Netty server 证明它触发双 GOAWAY 而不是应用级 `UNAVAILABLE`。send 归零后，活跃 Telemetry 收到正常 completion，ReceiveMessage/其他 streaming call 收到 retryable close intent；registry 必须等 wrapped listener `onComplete/onCancel` 才归零，正常结束、drain close、client cancel 三方竞态不重复 close、不出现负计数。
- `Server.awaitTermination=false`、中断或不服从 close 的长流触发可观测 force fallback；`forceServerShutdown()` once-only。正常路径必须先观察到 Server terminated 再回收 boss/worker EventLoopGroup；强制路径在第二次 await 仍失败时记录 `server_not_terminated`，仍必须在同一 stop deadline 内发起 EventLoopGroup 关闭并继续关闭其他 owner；正常 lease expiry、正常 non-send close 与 forced close 指标严格分开。
- Remoting 270–330 秒租约、5.3.2 门槛、GO_AWAY 在业务处理前；已入队未执行的 `RequestTask` 切 DRAINING 后仍被 accepted 计数覆盖。
- Remoting scoped admission context 在线程复用时不泄漏；submit reject、`stopRun`、`shutdownNow` 队列回收、channel-before-dispatch、channel-during-Broker、non-writable、write throw/failure 全部走 synthetic backend/双终态。
- 普通与真实 MultiProtocol Server 的基类 lifecycle handler 都必须生效；事件循环 intake barrier 后不能再提交 late `RequestTask`，writability callback 不能重开 auto-read；GO_AWAY/UNAVAILABLE/legacy rejection write 也计入 pending，直到 Future 终态。
- oneway 仅验证 best-effort 独立计数，不混入严格 send gate。
- Helm 渲染断言 legacy、admin-only bootstrap、strict 三套配置及 probe/PreStop、全链路 exec、600 秒 minReady、540 秒 grace、PDB、固定 `spec.replicas`、NLB 属性（含两条 connection-termination）、health-check protocol/port/path 指向 `/ready-for-traffic`、8082 不成为业务 listener 和 NetworkPolicy selector。
- 升级工具测试 `spec.replicas` 恒定渲染、**检测到 live HPA 即 fail** 的负例、最多一个 terminating old Pod、pause-on-failure、受控 rollback、一次 annotation bootstrap 和 timeout 公式。
- admin server 在 `POST /drain` flood 下 `/live`、`/ready`、`/ready-for-traffic` 的最大延迟；重入 `POST /drain` 复用同一 runId 且始终 202；`GET /drain/{runId}` 在 forced 终态返回可区分 `accepted_sends>0` 的结果。

### 5.2 客户端矩阵集成测试

5.0.7、5.2.1 和 Remoting 5.3.2 使用独立 JVM/容器启动，避免相同包名版本冲突。测试客户端只使用正式发布依赖，不修改 SDK 代码。统计对象是最终 logical send；底层 attempt/重试错误单独记录，不能把一次失败 attempt 当成最终 send 失败，也不能隐藏耗尽重试后的错误。

核心验收矩阵：

| 维度 | 必测值 |
|---|---|
| Provider | AWS NLB、ACK NLB |
| Client | gRPC 5.0.7、gRPC 5.2.1、Remoting 5.3.2 |
| Client 配置 | `maxAttempts=3`（默认）**与 `maxAttempts=1`**；`waitForReady=false` 的真实默认路径 |
| API | sync send、async send；batch 单独覆盖；oneway 按 §1.2 决策后单列 |
| Event | 3→5 扩容、单 Pod restart、全量 rollout、5→3 逐 Pod 缩容 |
| Load | 稳态低负载、目标生产负载、峰值突发；1 KiB、常规大小、5 MiB 消息 |
| Security | 当前生产 ACL/TLS 组合 |

Kubernetes 场景：

- 3 -> 5 扩容。
- 单 Pod restart。
- Deployment 全量滚动更新。
- 5 -> 3 逐 Pod 缩容。
- PreStop 重入、直接 SIGTERM、LB 摘流变慢、drain deadline 超时。
- 每个 gRPC 版本保持真实 Telemetry bidi stream 与 ReceiveMessage server stream 穿越单 Pod drain；确认第一阶段 GOAWAY 后新 RPC 转移，send 终态归零后旧 Pod 才正常完成 Telemetry、以 `UNAVAILABLE/[PROXY_DRAINING] reconnect` 截止 ReceiveMessage，客户端在其他 Pod 重建 Telemetry，旧 Pod 正常终止而非 force。
- Remoting 5.2.0/旧客户端存在时的降级路径。
- feature off、新旧 Proxy 混跑、annotation bootstrap、受控 rollback；supervisor 在存在 live HPA 时 fail 的负例。
- Broker 慢响应/flow control、Telemetry 长流、warmup NameServer 不可达、NetworkPolicy 开启和 provider 8082 health check。

每个 `provider × strict client × event` 核心 cell 至少执行 10 轮，累计不少于 1000 万次 logical acknowledged send；payload/security/fault 扩展场景每项至少 10 轮并报告样本数。基线使用同版本、同 offered load 的事件前 15 分钟稳定窗口；事件窗从 T0 到最后受影响 Pod STOPPED 后 60 秒。

- send 错误和超时为零观测。
- 事件窗 p99 不超过同负载基线 +100ms，p999 不超过基线 +500ms。
- `forced_close=0`、deadline/late-connection/transport-termination-failure 均无增量，Pod 无 SIGKILL。
- Remoting write Future 与 gRPC send stream terminal 前 permit 不得归零；报告不宣称服务端证明客户端收包。
- gRPC `Server.shutdown()`、active-call close、`awaitTermination` 与 EventLoop 回收的事件顺序必须可从测试探针/日志重建；`grpcOpenDrainableCalls` 在 close intent 后不得提前归零，正常矩阵中 `shutdownNow` 调用数必须为 0。
- rollout 实测同时 Terminating 的旧 Pod 不超过 1；supervisor 启动前确认目标 workload 无 live HPA（见 §4.1）。
- **重平衡指标使用连接数，不使用请求 QPS（复审 R1）。** 请求 QPS 受各客户端进程自身发送量分布影响，无法证明长连接是否重新分布。测量每 Pod 活跃**连接数**、transport 创建/关闭速率与**连接数 CV**，在同质化压测客户端下取值；时间窗按「一个完整 lease 周期 + grace + 余量」推导，不写死 6 分钟。请求 QPS 分布降级为观测项，不作门禁。
- 重复消息和 unknown outcome 单独计数，不作为零错误条件的替代；结果报告必须明确 at-least-once 语义。
- **零 observed 主动发送失败保持硬门禁，统计上界只作补充（复审 R6）。** 可额外报告在给定样本量与置信度下的失败率上界，但**不得**用 `<= 1e-6` 之类的上界偷换零失败目标。验证分层执行：deterministic unit → protocol integration → 真实客户端 E2E → 真实 NLB rollout → 长时间 soak。

### 5.3 验证命令

```bash
rtk mvn -pl remoting,proxy -am test
rtk mvn clean compile
rtk ur-format
rtk helm lint /Users/lossend/pro/rocketmq-helm
rtk pytest -q /Users/lossend/pro/rocketmq-helm/tests
```

最后在真实 AWS 与 ACK NLB 环境执行连接摘流和持续 send 验证；kind/本地测试不能替代 provider 验收。

## 6. 上线与回滚

首次从旧 Proxy 切换存在不可消除的 bootstrap 边界：旧进程没有租约、8082 和 drain 状态，新代码无法反向使其优雅。首次发布采用一次性受控流程，并**尽量压缩到一次有损 rollout**（复审 P1-8）：

1. 在独立 canary release 以生命周期开启状态验证 5.0.7、5.2.1、Remoting 5.3.2、真实 provider 8082 health check 和单 Pod 删除。
2. 低峰提前扩容主集群并确认容量余量（按 §4.1 的容量不等式，不按魔法副本数）；supervisor 前置检查确认目标 workload 无 live HPA。
3. **一次 annotation 驱动的 bootstrap rollout（复审 P1-8）：** 不再先改 template 再单独重启制造两次有损滚动。该轮同时发布具备新能力的二进制并开启 `enableProxyAdminServer=true`，通过 annotation 驱动完成一次受监控替换；完成后逐 Pod 验证 8082 可达，确保主集群已无不支持 health port 的旧镜像。该轮属于 bootstrap，只承诺尽力无感，不计入严格 SLO。
4. 确认所有现存 target 的 8082 healthy 后，在生产 values 显式开启 lifecycle，同时切换 HTTP probe（`/started`、`/ready`、`/ready-for-traffic`、`/live`）、PreStop、派生预算与 NLB health check。若该切换可与第 3 步合并为同一次 rollout 且渲染差异已由模板测试锁定，则合并；否则仍按 bootstrap 等级执行第二次替换。
5. 所有 Pod 都运行 lifecycle-enabled 配置后，等待至少一个完整最大租约+grace 周期，并确认 forced/timeout/late-connection 指标为零。
6. 才启用严格 SLO，并恢复正常受控 rollout。

出现 send SLO、连接风暴或 provider 摘流异常时，rollout supervisor 立即 pause 后续替换并保留现存 READY Pod。回退必须显式选择同时兼容二进制、probe 和 PreStop 的 Helm revision，并按相同串行 drain 流程执行；回退到无生命周期旧版本重新进入 bootstrap 等级，禁止自动 rollback。

## 7. 实施顺序与阶段出口

每一步独立提交、默认开关关闭；前一步出口未满足不得开始下一步：

1. **三个证据 spike（复审 Step 1）：** 任一 spike 失败先改架构，不进入批量编码。
   1. **gRPC**：锁定 grpc-java 1.53.0 `Server.shutdown()` 的内建双 GOAWAY、非阻塞返回、现存 Stream 保留、`awaitTermination`/`shutdownNow` 边界，并验证包装 `ServerCall` 后 drain close 与正常 send/close 的串行竞态。真实 Java Client 5.0.7/5.2.1 覆盖 shutdown 前后并发新 call、listener 拒连、in-flight call、连接重建，以及有/无空闲连接、单/多连接、不同并发度。
   2. **Remoting**：同一 NLB 地址重连、**第二次 `GO_AWAY`**、并发、batch、5 MiB、剩余 timeout 场景；transport replay 开/关与客户端版本边界。
   3. **Provider**：AWS、ACK、Kubernetes 的 endpoint/PreStop 并发传播与连接终止 **packet-level 抓包**，含 §4.2 表格中四条属性行为与 health-port 指向非业务 target port 的能力。
   
   产出必须包含按协议记录的 `migration_started`、`no_new_work_reached`、`admission_closed`、`accepted_inflight_zero`。**不允许**把 `awaitServerTermination()` 或「已发送 GOAWAY」单独当作跨协议屏障。
2. **冻结不变量与单一预算模型（复审 Step 2）：** 定义协议级 no-new-work 条件、admission/accepted-inflight/双终态定义、provider/app/PreStop/process 的派生公式（§3.6 命名预算），并**删除文档中的第二套关闭顺序**——全文只保留 §1.1 与 §3.4 的唯一顺序。

3. **关闭范围决策（复审 Step 3）：** 按 §1.2 选定 oneway 策略、Remoting `clientVersion × invocation mode` 矩阵、事务亲和性策略（§9.4），确认 ReceiptHandle/redelivery 是否进入严格门禁，并用生产流量 inventory 验证范围假设。

4. **生命周期核心：** 先写 `DrainSession`、`ProxyLifecycleCoordinator`、`SendDrainGate` 和并发测试，再引入 `ProxyRuntime` 所有权、readiness contributors 与管理接口；同时修复 admin waiter 饥饿（§3.2 的 `202 + runId` 契约）、为所有 executor/worker 增加 bounded stop、并为 supervisor 提供可达且可确认的 drain result 交接（§3.7）。出口是状态/CAS/deadline 测试全绿，feature off 行为不变。
3. **gRPC 接入：** 配置 max age/grace、tracer holder/filterContext/interceptor permit 绑定、backend/stream 双终态、late Attributes、non-unary active-call registry，以及阶段化 `initiate/close-non-send/await/force`；出口是内建双 GOAWAY 证据、Telemetry/ReceiveMessage 关闭竞态和 5.0.7/5.2.1 组件测试全部通过。
4. **Remoting 接入：** 在 `NettyRemotingAbstract` 增加默认 NOOP 的 pre-enqueue listener 和显式 `RequestTask` context，Proxy activity 接入 tracked writer、lease 与 event-loop barrier；出口是 5.3.2 严格路径和 legacy 降级路径通过。
5. **资源与信号：** 统一 runtime shutdown 顺序、执行器有界等待、TERM fallback，并修复 Chart、`mqproxy`、两个 `runserver.sh` 到 Java 的全链路 `exec`。
6. **指标与门禁：** 增加低基数 metrics、PrometheusRule、drain 日志和升级脚本查询；forced/late/deadline 任一增量可以自动 pause rollout。
7. **Helm：** 实现 schema/config、HTTP probes（含 `/ready-for-traffic`）、PreStop、派生预算、PDB/placement/NetworkPolicy、AWS/ACK 与 standalone 降级 profile；固定 `spec.replicas` 且不渲染 HPA。先做 render/lint/test，不读取或覆盖用户私有环境 values。
8. **升级与矩阵验收：** 实现 canary、一次 annotation bootstrap、live-HPA preflight、受控 rollout/rollback，最后跑两 provider 的客户端矩阵；只有完整报告满足第 5 节才宣布严格能力可用。

## 8. 代码级设计总览

本节把第 1–7 节下钻为可直接执行的实现契约，不降低前述安全不变量。实现中如类名或方法签名需要微调，应保持本节定义的所有权、线性化点和终态语义；不得用全局 map、固定 sleep 或静态 coordinator 替代。

### 8.1 包与文件边界

新增核心包 `org.apache.rocketmq.proxy.lifecycle`，协议适配分别放到 `lifecycle.grpc` 和 `lifecycle.remoting`，管理面放到 `lifecycle.admin`。规范文件如下：

```text
proxy/src/main/java/org/apache/rocketmq/proxy/lifecycle/
  ProxyLifecycleState.java
  ProxyLifecycle.java
  DrainPhase.java
  DrainTrigger.java
  DrainSession.java
  DrainRun.java
  DrainResult.java
  StopRun.java
  StopResult.java
  ProxyLifecycleSnapshot.java
  SendProtocol.java
  SendLifecycleContext.java
  SkipReason.java
  ProtocolResult.java
  ShutdownDeadline.java
  DeadlineAwareShutdown.java
  ExecutorShutdown.java
  SendDrainGate.java
  SendPermit.java
  ProxyLifecycleCoordinator.java
  ReadinessBarrier.java
  ReadinessContributor.java
  ReadinessResult.java
  FailureScope.java
  ConstructionScope.java
  ProxyRuntime.java
  ProxyRuntimeFactory.java
  ProxyLifecycleMetrics.java

proxy/src/main/java/org/apache/rocketmq/proxy/lifecycle/admin/
  ProxyAdminServer.java
  ProxyAdminResponse.java

proxy/src/main/java/org/apache/rocketmq/proxy/lifecycle/grpc/
  GrpcSendLifecycleHolder.java
  GrpcSendStreamTracerFactory.java
  GrpcSendLifecycleInterceptor.java
  GrpcActiveCall.java
  GrpcActiveCallRegistry.java
  GrpcActiveCallInterceptor.java
  GrpcDrainStatusPolicy.java
  GrpcTransportLifecycleFilter.java
  GrpcDrainAdapter.java

proxy/src/main/java/org/apache/rocketmq/proxy/lifecycle/remoting/
  RemotingSendLifecycle.java
  RemotingSendLifecycleListener.java
  RemotingDrainAdapter.java
```

跨模块的通用 Remoting 扩展只放在 `remoting` 模块，不能让 `remoting` 依赖 `proxy`：

```text
remoting/src/main/java/org/apache/rocketmq/remoting/netty/
  RequestAdmissionAware.java
  RequestIntakeController.java
  RequestAdmissionResult.java
  RequestTaskContext.java
  RequestTaskLifecycle.java
  TaskSkipReason.java
  RemotingRequestLifecycleListener.java
  RequestTask.java                         # 修改
  NettyRemotingAbstract.java               # 修改
  NettyRemotingServer.java                 # 修改
```

已有 Proxy 文件的职责调整：

| 文件 | 代码级改动 |
|---|---|
| `proxy/.../ProxyStartup.java` | 只保留参数解析、最终配置校验、创建一个 `ProxyRuntime`、安装捕获该实例的 shutdown hook；删除进程静态 `PROXY_START_AND_SHUTDOWN` 所有权。 |
| `proxy/.../config/ProxyConfig.java` | 连续增加第 3.5 节字段/getter/setter，以及一次性聚合错误的 `validateGracefulLifecycle()`。 |
| `proxy/.../common/ProxyContext.java` | 增加内部 typed accessor 保存 request-scoped send lifecycle；不写 wire 字段，不使用 client id/opaque 做 key。 |
| `proxy/.../common/ContextVariable.java` | 增加仅进程内使用的 `SEND_LIFECYCLE_CONTEXT` key。 |
| `proxy/.../grpc/GrpcServerBuilder.java` | 在 lifecycle 开启时配置 age/grace、send tracer/interceptor、active-call interceptor 和 transport filter；把自建 boss/worker `EventLoopGroup` 与原始 `Server` 一并原子移交给 `GrpcServer`，build 失败立即逆序释放，其他对象不保留原始 Server 引用。 |
| `proxy/.../grpc/GrpcServer.java` | 成为 `Server`、TLS reload listener 与 boss/worker groups 的唯一 owner；提供 `initiateServerDrain()`、`awaitServerTermination(deadline)`、`forceServerShutdown()`、`shutdownOwnedResources(stopDeadline)` 四段式幂等接口。正常 drain 只操作 Server；STOPPING 才注销 TLS listener 并回收 EventLoop，正常路径以 Server 已终止为前置，强制路径即使 Server 拒绝终止也不得跳过资源回收；feature-off 的旧 `shutdown()` 只作为兼容 adapter，严格路径不得调用。 |
| `proxy/.../grpc/v2/GrpcMessagingApplication.java` | 将 holder 保存到 `ProxyContext`；用 `executor.execute(new GrpcTask(...))` 保留 reject handler 的真实 task 类型；queued send runnable 在 Broker 调用前做 `backendStarted()`；Future 完成写 backend terminal。 |
| `proxy/.../grpc/v2/DefaultGrpcMessagingActivity.java`、`proxy/.../grpc/v2/channel/GrpcChannelManager.java` | activity 显式拥有并关闭 channel manager；manager 构造器不调度任务，周期任务移入 `start()`，并在共享 stop deadline 内停止 scheduler/channel。 |
| `proxy/.../grpc/v2/common/ResponseWriter.java` | 保持纯 observer writer，不在 singleton 中持有 permit；协议 canonical terminal 仍由 tracer 负责。 |
| `proxy/.../remoting/RemotingProtocolServer.java` | 构造并安装 lifecycle listener/adapter，只分类三种 producer send；提供 freeze/await/force 协议方法；构造器不得启动 lease/扫描任务，统一移入 `start()`。 |
| `proxy/.../remoting/activity/AbstractRemotingActivity.java` | 从 scoped `RequestTaskContext` 复制 lifecycle 到 `ProxyContext`；Broker 调用与响应写回补齐双终态。 |
| `proxy/.../remoting/activity/SendMessageActivity.java` | producer send 使用严格上下文；`CONSUMER_SEND_MSG_BACK` 明确不进入严格 gate。 |
| `proxy/.../metrics/ProxyMetricsManager.java`、`ProxyMetricsConstant.java` | 接受 `ProxyLifecycleMetrics` 实例并在同一 Meter 注册低基数 instruments；不再新增静态可变 lifecycle 计数器。 |
| `proxy/.../service/ClusterServiceManager.java` 及缓存/心跳子组件 | 构造/init 只装配，不启动 scheduler；全部周期任务移入 owner 的 `start()`，由 owner 在同一个 stop deadline 内关闭。 |

### 8.2 Runtime 所有权与构造顺序

`ProxyStartup.main` 的目标形态为：

```java
ProxyRuntime runtime = null;
try {
    CommandLineArgument argument = parseCommandLineArgument(args);
    initConfiguration(argument); // CLI override 后调用 validateGracefulLifecycle()
    runtime = new ProxyRuntimeFactory(ConfigurationManager.getProxyConfig()).create();
    ProxyRuntime shutdownTarget = runtime;
    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> shutdownTarget.shutdown(DrainTrigger.SIGTERM).join(), "ProxyShutdownHook"));
    runtime.start();
} catch (Throwable startupFailure) {
    if (runtime != null) {
        runtime.forceStop(startupFailure).join();
    }
    log.error("Proxy startup failed", startupFailure);
    System.exit(1);
    return;
}
```

实际 `main` 仍按现有方式记录日志和以非零码退出，但不得在 hook 内重新从 singleton 查组件。hook 应在 `runtime.start()` 前注册，使部分启动失败也能逆序关闭；hook 不允许 fire-and-forget，必须 `join()` 唯一 stop future，返回后 JVM 才可退出。`forceStop` 与正常 shutdown 共享 stop CAS，但不把启动失败伪装成正常 DRAINED。

`ProxyRuntimeFactory.create()` 只负责装配，不启动线程。它以 `ConstructionScope` 记录每个已构造的 `AutoCloseable`/executor/event-loop owner；只有 `ProxyRuntime` 完整创建后才 `commit()`。关键限制是 Java 会先求值 `scope.own(name, buildX())`：外层 scope 只能回收成功返回的对象，不能自动回收 `buildX()` 内部创建到一半的 leaf。因此每个复合 factory/constructor 自己也必须 exception-safe，用嵌套 scope 在每个 leaf 创建后立即登记，成功构造 owner 后才转移所有权。这样即使异常发生在 `runtime` 赋值前，`main` 的 null 分支也不会泄漏资源。顺序固定为：

```java
try (ConstructionScope scope = new ConstructionScope()) {
    ExecutorService serverExecutor = scope.own("server-executor", newServerExecutor());
    MessagingProcessor processor = scope.own("messaging-processor", buildMessagingProcessorSafely());
    GrpcServer grpc = scope.own("grpc-server-bundle", buildGrpcServerSafely(serverExecutor));
    RemotingProtocolServer remoting = scope.own("remoting-server", buildRemotingSafely(processor));
    ProxyAdminServer admin = scope.own("admin-server", buildAdmin());
    ProxyRuntime result = new ProxyRuntime(...);
    scope.commit();
    return result;
}
```

复合 factory 的模板为 `try (ConstructionScope local = ...) { leaf1=local.own(...); leaf2=local.own(...); Owner result=new Owner(leaf1, leaf2); local.commit(); return result; }`：Owner 构造前第 N 个 leaf 失败由 local 逆序关闭，Owner 构造成功后 leaf 所有权转给 Owner，外层再登记 Owner。`ConstructionScope` 提供 `<T extends AutoCloseable> T own(String, T)` 与 `<T> T own(String, T, CheckedCloseAction<T>)` 两种登记方式，后者覆盖现有未实现 `AutoCloseable` 的 owner/executor。`own` 只登记资源，不调用 start；rollback 捕获每个 close 异常并继续，最后把它们作为 suppressed exceptions 附到原始构造异常。Local 分支也必须先成功构造并登记 exception-safe 的 Broker wrapper，再开始构造 MessagingProcessor。runtime 构造成功后所有权一次性转移，scope 不得再关闭资源。

1. 创建 clock、`ProxyLifecycleMetrics`、`SendDrainGate`、`ReadinessBarrier` 和 coordinator。
2. 创建 server executor、MessagingProcessor、TLS manager、gRPC application。
3. 创建 gRPC send holder/tracer、active-call registry/interceptor、transport filter 和唯一持有原始 `Server`/EventLoopGroups 的 `GrpcServer`。
4. 创建 Remoting listener/adapter 和 `RemotingProtocolServer`。
5. 创建 admin server；admin 关闭时装配 NOOP 实现。
6. Cluster mode 将所有组件注入一个 `ProxyRuntime`；Local feature-off 分支还必须把现有 `BrokerController` wrapper 注入 runtime，不再向静态列表 append。

`ProxyRuntime.start()` 的 Cluster 正常顺序为 admin bind → metrics → TLS → MessagingProcessor → gRPC application → gRPC listener → Remoting listener → listener contributors complete → route warmup → READY。admin 提前 bind 便于 startupProbe 观察启动状态，但 `/started` 在两个业务 listener 成功绑定前返回 503。Local feature-off 必须保持现有兼容顺序：先启动 runtime 拥有的 `BrokerController` wrapper，再启动 MessagingProcessor/协议层，关闭时严格反序；专门的 regression test 锁定该分支。

`ProxyRuntime.beginDrain(trigger)` 与 `ProxyRuntime.shutdown(trigger)` 是两个不同的幂等入口：前者返回同一 `DrainRun`，只排空到 DRAINED/FORCE_DRAINING；后者返回同一 `StopRun.stopFuture`，必要时先加入/启动 drain，再唯一推进 STOPPING/STOPPED。具体关闭契约：

```java
public DrainRun beginDrain(DrainTrigger trigger);
public CompletableFuture<StopResult> shutdown(DrainTrigger trigger);
public CompletableFuture<StopResult> forceStop(Throwable startupFailure);
```

`shutdown` 的 CAS 顺序固定为：先在 lifecycle-on 路径调用 `beginDrain(trigger)` 取得唯一 `DrainRun`，由其 session 计算 stop budget；再构造完整 `StopRun(deadline, future)` 并 CAS 到 `stopRunRef`。CAS 失败直接返回获胜 future；CAS 获胜后若 drain 尚未完成，必须立刻调用 `coordinator.escalateForStop(stopDeadline)`，再启动一次性的 `ProxyStopCoordinator` 线程执行 orchestration。stop 不能提交到随后要由自己关闭/await 的 executor，否则会自等待死锁；该专用线程完成 future 后自然退出，不属于被关闭列表。若 `Thread.start()` 本身失败，获胜调用线程同步执行相同 closure。这样 HTTP/TERM 与两个 TERM 竞态时都不会出现“stop future 属于一个 session、deadline 属于另一个 session”。

1. lifecycle 开启：加入/创建 coordinator drain，等待至 `DRAINED` 或 `FORCE_DRAINING`。TERM 绕过/打断 PreStop 时，stop coordinator 先在 stop deadline 内等待 `escalateForStop` 的 freeze-issued future；该操作在 lifecycle scheduler 的首个可执行 tick 取消 lb/lease timer并同步越过等待阶段，立即 `closeAdmission`、向两个 adapter 发出 freeze。随后只在 `min(drain deadline, stop deadline)` 内等待 accepted send，到点 force。禁止先耗尽 30 秒再开始 freeze，也不得让 hook 等完整 450 秒。
2. lifecycle 关闭：复现原有协议先停、application/TLS/messaging/metrics 后停的兼容关闭顺序。
3. 两种模式最终都执行：Remoting → gRPC `Server`（若 drain 已终止则幂等确认，不重新取得固定 timeout；否则有序 await，到期/中断后 once-only `shutdownNow`并再用剩余预算 await）→ gRPC TLS reload listener → worker EventLoopGroup → boss EventLoopGroup → gRPC application/channel manager executors → `ThreadPoolMonitor.shutdown()` → shared server executor → MessagingProcessor/Cluster 子 owner → Local Broker wrapper → TLS → lifecycle/readiness executor → metrics → admin。第二次 Server await 仍失败时记录 `server_not_terminated`，但不得阻塞 TLS listener、EventLoopGroup 和后续 owner 的关闭尝试。仅关闭 runtime 实际拥有且已启动的组件；`ProxyStopCoordinator` 不得等待自身，任何阶段都不得绕过 `GrpcServer` 直接操作原始 `Server`。
4. 首次 shutdown 构造 immutable `StopRun`，语义上 `stopDeadline=min(now + proxyJvmShutdownTimeoutSeconds, drainSession.startedNanos + proxyPreStopWaitSeconds + proxyJvmShutdownTimeoutSeconds)`；实现不能直接比较可能 wrap 的绝对 nano 值，而是计算 `min(jvmTimeout, (preStopDeadline + jvmTimeout) - now)` 的非负 remaining duration 后再构造 deadline。无 drain session 的启动失败使用完整 JVM timeout。单个 `AtomicReference<StopRun>` CAS 发布 future 与 deadline，所有 `DeadlineAwareShutdown` 收到同一实例。
5. `ExecutorShutdown` 统一执行 `shutdown()`、`awaitTermination(stopDeadline.remaining())`、必要时 `shutdownNow()`。禁止每个子组件重新获得 30 秒；回收出的 task 必须逐个触发其 lifecycle skip hook。
6. 任何关闭异常收集到 `StopResult.causes`，继续关闭剩余资源；最终状态不能因为日志异常而卡在 STOPPING。
7. interrupt flag 是线程私有状态，禁止把“恢复中断”转交给另一个 coordinator/executor 线程。每个实际执行阻塞 await 的最外层 orchestration task（普通 `DrainRun` task 或专用 `ProxyStopCoordinator` task）都使用自己的 thread-confined `interrupted` 布尔值：任何阻塞调用抛出 `InterruptedException` 时记录当前 owner/cause，利用 Java 已清除的 interrupt bit 继续本 task 负责的非阻塞 force 与后续有界回收；adapter/子 owner 不得立即 re-interrupt 使后续 await 全部短路。该 task 先完成它自己的 result/future，再在同一线程的最外层 `finally` 中恢复 `Thread.currentThread().interrupt()` 并立即退出，避免 inline completion 回调带着 interrupt flag 运行。跨线程只传播 cause/result；接收方只处理自己线程的中断。无论是否中断都必须尝试本 task 负责的每个 owner 各一次。

不要修改公共 `AbstractStartAndShutdown` 的语义；Broker、NameServer 等其他进程不应被本功能改变。

**`ThreadPoolMonitor` 必须改为 deadline-aware（复审 P1-5）。** 当前 `ThreadPoolMonitor.shutdown()` 只调用 `shutdown()`，没有 bounded await 也没有 force stop，且原资源清单未点名修改这个类。后果有两个方向：`STOPPED` 证据会早于真实资源收尾（丢失最后的清理/观测结果）；而若简单地在 hook 中加入**无界** await，卡住的任务会直接耗尽 Pod grace。

因此每个 process-static executor、listener、client、worker 与 scheduler 都必须：

1. 枚举唯一 owner（不得有两处关闭同一资源，也不得无人关闭）；
2. 执行 `shutdown() -> await(remaining) -> shutdownNow()/close()` 三段式；
3. 所有 await **只消费同一个绝对 `StopDeadline` 的剩余时间**，任何阶段或子组件都不得重新获得完整预算；
4. 记录首次失败后继续 best-effort 清理其余资源，不因单点失败短路；
5. 测试覆盖重复 stop、部分 start 失败、卡死 task 与 forced terminal。

#### ReceiptHandle：缺口是 bounded await 与可观测性，不是「只等自然过期」（复审 P1-2）

`DefaultReceiptHandleManager.shutdown()` **已经**调用 `clearAllHandle()`，并通过 `CLEAR_GROUP` 主动执行 change-invisible-time。因此「关闭时只等待自然过期」这一判断不成立，不得作为设计前提。真实缺口是：

- 收集并等待现有 clear task/Future，受统一 `StopDeadline` 约束（当前是 fire-and-forget）；
- 超时后记录 remaining handle/group 数与 forced 原因；
- 验证 schedule/renew/return 三个 worker 的停止顺序（其中两个当前未注册到关闭链）；
- 用 redelivery/重复消费指标评估影响，而不是断言零影响。

消费语义是否进入严格发布门禁属于范围决策，见 §1.2。

Cluster strict 路径需要把 deadline 继续传入嵌套 owner，而不只等待最外层三个 executor。`DefaultMessagingProcessor`、`ClusterServiceManager` 及其子组件实现 proxy-only `DeadlineAwareShutdown.shutdown(ShutdownDeadline)` overload；原 `StartAndShutdown.shutdown()` 保留给 feature-off/其他调用方。`RemotingProtocolServer`、`ClusterServiceManager` 和 `GrpcChannelManager` 当前在构造/init 中启动的调度全部搬到 `start()`；`DefaultGrpcMessagingActivity` 成为 `GrpcChannelManager` 的明确 owner。首批必须覆盖：MessagingProcessor producer/consumer pools、ClusterServiceManager scheduler、TopicRoute/Metadata cache refresh、ReceiptHandle schedule/renew/return、HeartbeatSyncer、ClusterTransaction heartbeat、GrpcChannelManager、gRPC application 五个 pool、Remoting 六个 pool/timer、shared gRPC server executor 和 process-static `ThreadPoolMonitor`。processor 中仅引用外部 executor 的字段不重复关闭。

### 8.3 核心接口与状态表示

Java 8 不使用 `record`。`DrainSession` 是 final immutable value object，字段至少包括：

```java
public final class DrainSession {
    private final String drainId;
    private final DrainTrigger trigger;
    private final long startedNanos;
    private final long lbCutoffNanos;
    private final long migrationCutoffNanos;
    private final long hardDeadlineNanos;
    private final long preStopDeadlineNanos;
    private final Instant startedAtForLogOnly;

    long remainingNanos(DrainPhase phase, long nowNanos, long callerDeadlineNanos);
    long stopBudgetNanos(long nowNanos, long jvmTimeoutNanos);
    boolean isExpired(DrainPhase phase, long nowNanos);
}
```

cutoff 只在首次 `beginDrain` 时由已校验的短 duration 加到同一个 `nanoTime` 基准；`preStopDeadlineNanos=T0+proxyPreStopWaitSeconds` 不是 drain hard deadline，只用于 HTTP 等待上限和 TERM stop cap。过期与剩余时间一律用 `deadlineNanos - nowNanos` 的差值判断，不能直接比较绝对 nano 值，测试覆盖 `long` wrap-around。重复 HTTP drain 或 SIGTERM 不改字段；调用方自己的更短 deadline 只将两者的 remaining duration 取 `min`，绝不能写回并延长 session。测试注入 `LongSupplier nanoClock` 和 `java.time.Clock`，不在生产代码调用可 mock 的全局静态时间。

session 与 completion 必须作为不可拆分对象发布；Java 8 形式为：

```java
public final class DrainRun {
    private final DrainSession session;
    private final CompletableFuture<DrainResult> drainFuture;
}

public final class StopRun {
    private final ShutdownDeadline stopDeadline;
    private final CompletableFuture<StopResult> stopFuture;
}
```

`ShutdownDeadline` 同样是 final immutable value object，内部保存同一 monotonic `LongSupplier` 与首次构造的 `deadlineNanos`，至少提供 `long remainingNanos()` 和 `boolean isExpired()`；`remainingNanos()` 只计算 `Math.max(0, deadlineNanos - nanoClock.getAsLong())`，任何子组件不得传入 duration 后自行重建 deadline。测试覆盖已过期、连续调用单调不增和 `nanoTime` wrap-around。

`DrainResult` 只描述正常 DRAINED 或 forced drain 结果；`StopResult` 描述资源关闭异常/超时，不能混用一个 future 让 HTTP drain 意外触发进程停止。

`ProxyLifecycleCoordinator` 的外部面保持小而稳定：

```java
public interface ProxyLifecycle {
    DrainRun beginDrain(DrainTrigger trigger);
    ProxyLifecycleSnapshot snapshot();
    boolean isStarted();
    boolean isLive();
    boolean isReady();
    void markFatal(String component, Throwable cause);
}
```

实现提供单一 public `onStartupComplete()`，由 `ProxyStartup` 在完整组件启动链成功返回后原子发布 STARTING→READY；内部保留 package-private 的 `transition(expected, next, reason)` 和 `CompletableFuture<Void> escalateForStop(ShutdownDeadline stopDeadline)`。状态存入单个 `AtomicReference<ProxyLifecycleState>`；合法边由显式 map/`switch` 校验，任何真实回退、跳过正常边或无 token 的重复推进都抛出并计数，不能仅打印 warn。lb/lease scheduled callback 必须携带 phase generation；TERM 取消与 timer 已入队竞态时，旧 generation 回调安静 no-op，这属于预期取消竞态而非 invariant violation。

`beginDrain` 先在本地构造完整 `DrainRun(session, future)`，再用单个 `AtomicReference<DrainRun>.compareAndSet(null, candidate)` 发布；失败者直接返回获胜对象。禁止两个 atomic 分别发布 session/future，否则读者可能观察到 torn pair。获胜线程只向单线程 `ScheduledExecutorService` 提交一次 orchestration；阶段等待使用可取消 scheduled future，不能在 lifecycle thread 上 `sleep`/阻塞到 cutoff。HTTP、PreStop 和 hook 只 join `drainFuture`，不各自执行 drain 步骤。`ProxyRuntime` 另以单个 `AtomicReference<StopRun>` 发布 stop deadline/future。

`escalateForStop` 只收紧、不修改 immutable `DrainSession`：它以 CAS 保存最短 `stopDeadline` override，取消尚未触发的 lb/lease scheduled future，并在 lifecycle scheduler 首个 tick 按合法边快速推进。READY 依次执行 READY→QUIESCING→MIGRATING→DRAINING，QUIESCING/MIGRATING 从当前位置推进到 DRAINING；STARTING/部分启动直接进入 FORCE_DRAINING 并关闭已启动协议。到 DRAINING 的动作仍是同一个 once-only gate close + 两协议 fan-out freeze。方法返回的 future 只在两个 freeze 调用均已发出后完成，供 stop coordinator 证明“先冻结、再等待”。DRAINED/FORCE_DRAINING 调用为幂等 no-op。

从 stop override 发布起，所有尚未开始或仍在等待的 phase 使用 `min(session phase deadline, stopDeadline)`；forced adapter 也必须接收这个 effective deadline。正常 HTTP drain 未见 stop override 时，force close 可使用 `preStopDeadline` 的剩余预算；direct TERM 路径绝不能再使用约 480 秒的 PreStop 预算突破 StopRun。

coordinator 的 DRAINING 段必须是 fan-out/fan-in，禁止先完整等待一个协议再启动另一个协议：

1. T0 进入 QUIESCING 并同步撤 readiness，两个 adapter 同时收到 quiesce 通知，gate 仍开放。
2. 固定 lbCutoff 进入 MIGRATING，开始拒绝/统计 late transport；继续等待存量 lease，任何连接不改 session cutoff。
3. business transport 已归零可提前进入 DRAINING，否则到 migrationCutoff 强制进入。
4. **migration_started**：并发向两个协议发起迁移——gRPC adapter 的 `stopAcceptingNewRpcs()`（内部只委托 `GrpcServer.initiateServerDrain()`，非阻塞并触发 grpc-java 内建双 GOAWAY，不等待 PING ACK 或 transport termination）与 Remoting `freezeRequestIntake()`。此时 gate 仍开放，先全部发出再各自 await。
5. **no_new_work_reached**：每个协议独立到达 §3.4「唯一关闭顺序」表验证的 no-new-work 谓词并完成 intake barrier。谓词由 §7 Step 1 spike 填入；退化为有界 quiet 窗口时受同一 effective deadline 约束。两个协议并发推进，任一协议不得因等待另一协议才开始/完成迁移。
6. **admission_closed**：由**最后**到达 no-new-work 的协议触发唯一一次共享 `sendDrainGate.closeAdmission()`；此前 gate 始终开放，不得提前关闭。
7. **accepted_inflight_zero**：admission 关闭后并发等待共享 `gate.drainedFuture()`、`grpcOpenSendRpcs==0` 与 `remotingPendingWrites==0`；任一协议不得因等待共享 gate 而阻止另一协议收尾。
8. send 终态全部完成后，并发关闭 Remoting business channels，并调用 `GrpcActiveCallRegistry.closeAll(GrpcDrainStatusPolicy)` 按 RPC 类型向 gRPC 非 unary call 发出一次 close intent；等待 `grpcOpenDrainableCalls==0` 后用剩余 effective deadline 调用 `GrpcServer.awaitServerTermination()`。close intent、GOAWAY、`ServerCall.close()` 返回都不是 terminal，必须等 wrapped listener/transport 的真实终态。
9. 所有条件成功才转 DRAINED；任一 phase future 异常或 effective deadline 超时 CAS 到 FORCE_DRAINING，同时触发两个协议 force close。force close 最多使用 `min(preStopDeadline, stop override if present)-now`，不得关闭 admin/metrics/共享 JVM owner；无论结果都以 causes 完成 forced `drainFuture`。coordinator 本身停在 DRAINED/FORCE_DRAINING；只有 `ProxyRuntime.shutdown()` 可继续推进 STOPPING。

### 8.4 Readiness 实现

接口定义为：

```java
public interface ReadinessContributor {
    String name();
    CompletableFuture<ReadinessResult> check();
    FailureScope failureScope(); // LOCAL_FATAL 或 SHARED_DEPENDENCY
}
```

首批 contributor：`grpc-listener`、`remoting-listener`、`messaging-started`、`tls-context`、`nameserver-route` 和每个 `warmup-topic:<topic>`。每个 warmup topic 先调用现有 `MessagingProcessor.getTopicRouteDataForProxy(...)` 证明 NameServer 路由，再调用 `MetadataService.getTopicMessageType(...)`；Cluster 实现会通过 Broker `getTopicConfig(...)` 建立连接且不发送消息。空路由、`UNSPECIFIED`、异常或 timeout 都算失败；以固定上限指数退避重试，直到 `proxyWarmupTimeoutSeconds` 后保持 STARTING/NotReady，后续仍继续低频重试。

`ReadinessBarrier` 只允许第一次 `all required success` 将 STARTING 推为 READY。READY 后两个端点分别求值（谓词见 §3.2）：

| | `/ready`（kubelet/EndpointSlice） | `/ready-for-traffic`（provider health check） |
|---|---|---|
| `SHARED_DEPENDENCY` 失败 | **fail-open**，只更新 snapshot/metric | **fail-close**，返回 503 |
| `LOCAL_FATAL` | 立即 fail-close | 立即 fail-close |
| QUIESCING 之后 | 永不恢复，即使 contributor 重新成功 | 见下方条件式 200 |

`SHARED_DEPENDENCY` 的 fail-close 必须带可调阈值（连续失败次数/持续时长），避免单次抖动触发；阈值与 `D_dependency_wait`（§3.6）一致派生。

`/ready-for-traffic` 在计划内 QUIESCING/DRAINING 期间是否继续返回 200，只有 §3.2 三个条件同时成立才可采用；STARTING 与 STOPPED 必须返回 503。默认实现取保守值（drain 期间即返回 503），只有在 §4.2 spike 证明 provider 不会因 unhealthy 终止既有连接后，才允许切到条件式 200 —— 该切换必须是显式配置项，不得由代码默认。

admin-only bootstrap 模式不执行严格 warmup gate；两个业务 listener 已 bind 且无 local fatal 即 `/ready=200`，snapshot 明确 `lifecycleEnabled=false`。

## 9. Send gate 与协议适配的精确代码契约

### 9.1 `SendDrainGate` 与 `SendPermit`

`SendDrainGate` 的 `AtomicLong state` 使用最高位作 CLOSED，低 63 位作 accepted count：

```java
Optional<SendPermit> tryAcquire(SendProtocol protocol);
long closeAdmission();
boolean isAdmissionClosed();
long acceptedCount();
CompletableFuture<Void> drainedFuture();
```

`tryAcquire` 的成功 CAS 是 send admission 线性化点；发现 CLOSED 立即返回 empty。`closeAdmission` CAS 设置关闭位并返回同一 state 中的 count；设置后 count 为零时完成 `drainedFuture`。count 达上限必须 fail-fast，不允许溢出到关闭位。

每个 `SendPermit` 用单个 `AtomicInteger` 编码 backend 状态、protocol terminal 位和 RELEASED 位：

```text
backend: NOT_STARTED(0) -> STARTED -> TERMINAL
                         -> SKIPPED
protocol: OPEN -> TERMINAL
release:  backend in {TERMINAL, SKIPPED} && protocol=TERMINAL -> RELEASED
```

方法契约：

```java
boolean backendStarted();              // 只有 NOT_STARTED -> STARTED 返回 true
void backendTerminal(Throwable cause); // 首次只允许 STARTED -> TERMINAL；TERMINAL 重入为幂等 no-op
boolean tryBackendSkipped(SkipReason reason); // 只有 NOT_STARTED -> SKIPPED 返回 true
void protocolTerminal(ProtocolResult result);
CompletableFuture<Void> completionFuture(); // permit RELEASED 时只完成一次
```

`backendStarted()` 返回 false 时调用方不得接触 Broker，其中观察到 SKIPPED 是正常 cancel-before-dispatch 竞态。`tryBackendSkipped()` 在 NOT_STARTED 时由首个 reason 获胜并返回 true；已 SKIPPED 为幂等 false，已 STARTED/TERMINAL 也返回 false 且不改状态，表示 Broker 已经或曾经开始，调用方必须等待/沿用真实 Future terminal，不能把正常竞态计为 invariant failure。`backendTerminal()` 从 NOT_STARTED/SKIPPED 调用才是不可能路径，必须增加 invariant violation 并在测试/strict profile fail loudly；TERMINAL 重入为 no-op。释放 permit 只有一个 CAS 获胜并递减 gate；任何重复 callback 不得下溢。

### 9.2 gRPC 代码路径

本仓库 gRPC Java 为 1.53.0。第一步先用编译测试锁定 `NettyServerBuilder.maxConnectionAge(...)`、`maxConnectionAgeGrace(...)`、`addStreamTracerFactory(...)` 与 `addTransportFilter(...)` 的真实签名，再接生产 wiring。

必须把 grpc-java 已有能力与 Proxy 新增能力分开：`io.grpc.Server.shutdown()` 是非阻塞的有序关闭入口，Netty transport 对每条 HTTP/2 连接自动执行如下两阶段协议；`awaitTermination(...)` 只等待终止，既不发送 GOAWAY，也不配置下列 10 秒 PING fallback：

```text
Server.shutdown()
  -> GOAWAY(lastStreamId = Integer.MAX_VALUE, error = NO_ERROR)
  -> PING(0x97ACEF001)
  -> PING ACK，或 10 秒 fallback
  -> GOAWAY(lastStreamId = remote.lastStreamCreated, error = NO_ERROR)
  -> 等现存 stream 终止后关闭 connection
```

第一帧只通知客户端停止在旧 transport 创建 stream；PING ACK 是双向有序屏障，使第二帧可以安全冻结服务端实际接受的最高 stream id。`maxConnectionAgeGrace` 只约束 age-triggered connection close，不能当成 app-requested `Server.shutdown()` 的 await/force deadline；严格排空使用 `DrainSession/StopRun` 的 effective deadline，不能复用固定 `grpcShutdownTimeSeconds`。

原始 `io.grpc.Server` 不暴露给 adapter/coordinator。`GrpcServer` 提供以下 package-private 阶段化契约；方法名可微调，但所有权与拆分不能改变：

```java
void initiateServerDrain(); // once-only server.shutdown()，不阻塞
boolean awaitServerTermination(ShutdownDeadline effectiveDeadline) throws InterruptedException;
void forceServerShutdown(); // once-only server.shutdownNow()
ProtocolResult shutdownOwnedResources(ShutdownDeadline stopDeadline);
```

核心实现必须保持下面的非阻塞/剩余预算语义，不能再写成 `server.shutdown().awaitTermination(fixedTimeout, unit)` 链式调用：

```java
void initiateServerDrain() {
    if (serverDrainStarted.compareAndSet(false, true)) {
        server.shutdown();
    }
}

boolean awaitServerTermination(ShutdownDeadline deadline) throws InterruptedException {
    long remainingNanos = deadline.remainingNanos();
    return remainingNanos > 0
        ? server.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)
        : server.isTerminated();
}

void forceServerShutdown() {
    if (forceStarted.compareAndSet(false, true)) {
        server.shutdownNow();
    }
}
```

这是对现有 `GrpcServer.shutdown()` 的拆分与收口，不是再叠加一套 GOAWAY 机制。调用边界固定为：

| 调用方/阶段 | 允许调用 | 禁止行为 |
|---|---|---|
| strict DRAINING | `initiateServerDrain()` 一次，之后用 session deadline 调 `awaitServerTermination()` | 不调旧 `shutdown()`，不在 drain 阶段注销 TLS listener/关 EventLoopGroup |
| strict FORCE_DRAINING/STOPPING | 在同一 effective deadline 上 `forceServerShutdown()`、`shutdownOwnedResources()` | 不再获取一个完整 `grpcShutdownTimeSeconds`，不绕过 owner 直接操作 raw Server |
| lifecycle-off 兼容路径 | 现有 `void shutdown()` 只构造一次 legacy deadline，再顺序委托 `initiate -> await/force -> shutdownOwnedResources` | 不再保留链式固定 await、忽略 false、吞中断或漏关 EventLoopGroup |

因此，strict 路径只有第一个 `serverDrainStarted` CAS 获胜者调用底层 `Server.shutdown()`；后续 STOPPING 仅观察/强制并回收 owner，不会重复编排 GOAWAY。

`initiateServerDrain()` 不注销 TLS listener、不关闭 EventLoopGroup；`awaitServerTermination` 只消费传入 deadline 的剩余时间并原样返回 boolean，绝不吞掉 `InterruptedException`。实际调用 await 的 orchestration task 捕获中断后，把它记录为 forced cause，调用 once-only force 并继续该 task 负责的有界清理，但不立即恢复 interrupt flag；它必须在同一执行线程的最外层 `finally` 恢复后退出，不得委托另一线程代为恢复。`forceServerShutdown()` 可与正常终止竞态但只能调用底层一次。

`shutdownOwnedResources()` 只在 STOPPING 使用，且不得把“Server 必须终止”变成阻断后续 owner 关闭的无界前置：

1. 若 Server 尚未终止，先确保已调用 `initiateServerDrain()`，并仅在 `stopDeadline.remainingNanos()` 内 await。
2. 返回 false 或抛出中断时记录 cause，调用 once-only `forceServerShutdown()`，然后再仅用同一 deadline 的剩余时间做一次 best-effort await。
3. 第二次 await 仍为 false，或 deadline 已归零，记录低基数 cause `server_not_terminated`，不再阻塞。
4. 无论 Server 是否终止，都 once-only 注销 gRPC TLS reload listener，并按构造逆序对 worker、boss 发起 `shutdownGracefully(0, Math.max(0, remainingNanos), TimeUnit.NANOSECONDS)`；只在尚有剩余预算时 await，任何 false/interrupt 都进入 `ProtocolResult.causes`。即使 remaining 为 0，也必须发起两个 group 的关闭，不做阻塞等待。
5. 返回详细 `ProtocolResult` 后由 runtime 继续关闭其他 owner，不因为 gRPC transport 不服从而卡住全 JVM。

feature-off 的 `StartAndShutdown.shutdown()` 用旧配置构造一次兼容 deadline 并委托这些阶段，不能继续保留“忽略 false 后打印 success”的实现。

`GrpcSendStreamTracerFactory` 使用生成代码 `MessagingServiceGrpc.getSendMessageMethod().getFullMethodName()` 匹配 send，不手写方法名字符串：

1. `newServerStreamTracer` 创建 `GrpcSendLifecycleHolder` 并立即增加 `grpcOpenSendRpcs`。
2. tracer 的 `filterContext(Context)` 用静态 `Context.Key<GrpcSendLifecycleHolder>` 注入该 holder。
3. `streamClosed(Status)` 先调用 `tryBackendSkipped(STREAM_CLOSED_BEFORE_DISPATCH)`，再写唯一 canonical protocol terminal并减少 open RPC；若 CAS 输给已 STARTED 的 backend，只写 protocol terminal并等待真实 Future。重复调用幂等。
4. 非 send 方法返回无计数 tracer，不接触 gate。

holder 明确区分 `bindPermit(permit)` 与 `rejectBeforeAdmission(reason)`：late/gate-closed RPC 没有 permit，只完成 synthetic backend 状态并等待 streamClosed 减 open RPC；正常 RPC 只能 bind 一次。任何已经从 gate 获取但 bind 失败的 permit 必须立刻走 SKIPPED + protocol terminal补偿，不能泄漏 accepted count。

`GrpcSendLifecycleInterceptor.interceptCall(...)`：

1. 非 send 直接 `next.startCall`。
2. send 必须从当前 Context 取得 tracer holder；缺失/重复绑定记 invariant violation 并以 `Status.INTERNAL` 关闭。
3. 从 `ServerCall.getAttributes()` 检查 transport late；late 或 gate 已关闭时 holder 调用 `tryBackendSkipped`，以 `Status.UNAVAILABLE` 关闭，禁止调用 handler。
4. 正常路径 `gate.tryAcquire()` 并 CAS bind 到 holder，然后才调用 handler。
5. wrapped listener 的 cancel/deadline 只记录原因；若 backend 尚未开始可 CAS 为 SKIPPED，但 protocol terminal 仍等 tracer `streamClosed`。

`GrpcActiveCallInterceptor` 使用 `ServerCall.getMethodDescriptor().getType()` 分类，不维护手写 method-name allowlist：UNARY 不进入 registry；`SERVER_STREAMING`、`CLIENT_STREAMING`、`BIDI_STREAMING` 全部在 `next.startCall` 前登记，所以当前 Telemetry、ReceiveMessage、尚未实现即快速返回的 PullMessage，以及未来新增 streaming RPC 都不会漏掉。它把原 call 包装成 `GrpcActiveCall` 后再传给后续 handler，并包装返回的 listener：

```text
OPEN -> CLOSE_INTENT_SENT -> TERMINAL
OPEN ----------------------> TERMINAL
```

- `GrpcActiveCall` 的 `sendHeaders`、`sendMessage`、正常 `close` 和 drain `closeForDrain` 必须在同一 per-call lock/serial section 内调用 delegate，禁止 coordinator 线程与业务响应并发操作非线程安全的 call。首次 close intent 获胜；后续正常/drain close 为幂等 no-op，并记录首个 status/reason。
- registry 的完成条件必须是 `registrationClosed && openCount==0`，不能在构造时因为初始 count 为 0 就预完成 `drainedFuture`。实现使用一个 packed `AtomicLong`（关闭位 + open count）或等价的单原子 immutable state，使 register/closeAll 线性化：register 只能在关闭位为 0 时 CAS 增加；`closeAll` 先 CAS 关闭位，再读取同一状态决定是否完成 future。禁止用独立 boolean/count 产生 torn observation。
- close intent 只阻止后续 outbound write，不从 registry 删除。只有 wrapped listener 的 `onComplete` 或 `onCancel` 首次 CAS 到 TERMINAL，才移除 handle、减少 `grpcOpenDrainableCalls`；只有该次递减同时观察到 registrationClosed 且新 count 为 0，或 `closeAll` 关闭入口时原子观察到 count 为 0，才完成一次 `drainedFuture`。`ServerCall.close()` 返回不能替代该终态。
- 如果 `next.startCall` 同步抛错，必须补偿 TERMINAL 并移除；注册后、listener 返回前收到并发 drain 时，close intent 仍由同一个 handle 串行化，不能泄漏。
- `GrpcActiveCallRegistry.closeAll(policy)` 先 CAS 关闭注册入口，再由 `GrpcDrainStatusPolicy` 根据 descriptor 为快照中的 handle 选择 close status：只对生成代码 `MessagingServiceGrpc.getTelemetryMethod().getFullMethodName()` 返回 `Status.OK`，其他非 unary 方法默认返回 `UNAVAILABLE/[PROXY_DRAINING] reconnect`。关闭入口返回后新的非 unary call 不进入 registry、不增加 open count，也不得进入业务 handler，而是按同一 policy 直接关闭并返回 no-op listener，最终 transport 终态仍由 Server/filter barrier证明。Telemetry 的 OK completion 必须以 `close_mode=completed` 记账，不能静默吞掉；其他流不能使用 OK 掩盖截断，也不能把任何 non-send close 计入 send permit。
- `GrpcServerBuilder.configInterceptor()` 必须用组件测试锁定 interceptor 顺序：transport Attributes 对 send interceptor 可见，send holder 仍来自 tracer Context，active-call wrapper 覆盖所有后续 handler；禁止依赖多次 `.intercept(...)` 的主观顺序猜测。

`GrpcMessagingApplication.sendMessage(...)` 在 service 线程把 current holder 存入新建 `ProxyContext`，不能依赖 worker executor 自动传播 `io.grpc.Context`。producer executor 中的 runnable 必须按以下顺序：

```java
SendLifecycleContext lifecycle = context.getSendLifecycleContext();
if (!lifecycle.backendStarted()) {
    return; // cancel/reject 已在 dispatch 前终止，绝不调用 Broker
}
try {
    grpcMessagingActivity.sendMessage(context, request)
        .whenComplete((response, error) -> {
            lifecycle.backendTerminal(error);
            writeResponse(context, request, response, observer, error, responseCreator);
        });
} catch (Throwable synchronousFailure) {
    lifecycle.backendTerminal(synchronousFailure);
    writeResponse(...);
}
```

auth/context validation 抛错、`executor.execute` 抛错和 `GrpcTaskRejectedExecutionHandler` 都调用 `tryBackendSkipped`；false 只表示本次调用没有赢得 NOT_STARTED→SKIPPED CAS，可能是 earlier-SKIPPED，也可能是 STARTED/TERMINAL，不能据此声称 worker 已开始或再合成 backend terminal。若日志/测试必须区分，读取 holder 的只读 backend snapshot，不据此修改状态。必须把 `addExecutor(...)` 中的 `executor.submit(new GrpcTask(...))` 改为 `executor.execute(new GrpcTask(...))`，确保 reject handler 与 `shutdownNow()` 观察到原始 `GrpcTask`，而不是 `FutureTaskExt` 包装导致现有 `instanceof GrpcTask` 永远不命中。`GrpcTask` 已持有 `ProxyContext`，不另建 observer→permit map。`ResponseWriter` 只表示 response intent；不得在 `onCompleted()` 时释放 protocol terminal。

`GrpcTransportLifecycleFilter.transportReady` 返回带以下 Attributes 的新对象：connection id、ready monotonic time、`lateAfterLbCutoff`。`transportTerminated` 只更新连接计数/await barrier，不释放 send permit，也不假设能 close transport。

`GrpcServerBuilder` 当前自行创建 Netty boss/worker `EventLoopGroup`。新 builder result 必须把原始 `Server`、boss group、worker group 作为一个 ownership bundle 原子移交给 `GrpcServer`：Netty build 抛错时 builder 按 worker → boss 逆序 `shutdownGracefully`；`GrpcServer.start()` 抛错时 runtime 同样回收 bundle。移交成功后 builder、`GrpcDrainAdapter`、coordinator 和 runtime 均不得保存原始 `Server` 或 EventLoopGroup 引用。正常 HTTP drain 只通过 `GrpcServer` 调用/等待 Server，不注销 TLS listener、不关闭 EventLoop；进入 STOPPING 后的正常顺序是 `Server terminated -> unregister TLS listener -> worker group -> boss group`，强制顺序则是 `shutdownNow + bounded await -> record server_not_terminated if needed -> unregister listener -> initiate worker/boss shutdown`。两者全部使用同一 `StopRun.stopDeadline`，false/interrupt 写 forced cause，强制路径不以 Server terminated 作为继续回收的前置。

`GrpcDrainAdapter`：

- QUIESCING/MIGRATING 只改变 coordinator 观察状态，已有 transport 继续服务。
- 遵循 §3.4/§8.3 的唯一顺序：进入 DRAINING 时 adapter 先调用 `grpcServer.initiateServerDrain()`（`migration_started`）停止新 RPC，该调用只触发内建双 GOAWAY 并立即返回；此时 gate 仍开放。达到 spike 验证的 `no_new_work_reached` 谓词后，coordinator 才在两个协议都就绪时触发唯一一次共享 `gate.closeAdmission()`，adapter 不得自行提前关 gate。
- admission 关闭后等 `gate.drainedFuture` 与 `grpcOpenSendRpcs==0`；只有 acknowledged send 的 backend/protocol 都 terminal 后，才调用 `activeCallRegistry.closeAll(drainStatusPolicy)`，避免 non-send close 路径扰动尚未完成的 send。
- 再等 `grpcOpenDrainableCalls==0`，然后以同一 effective deadline 调用 `grpcServer.awaitServerTermination()` 并等待 transport filter 的 terminated barrier。两者都完成且保留有界 grace 才返回正常 `DrainResult`。
- `awaitServerTermination` 返回 false、被中断，active-call registry 未归零或 transport barrier 超时，都记录具体 force cause，调用 once-only `grpcServer.forceServerShutdown()` 并返回 forced `DrainResult`；force 后只在 effective deadline 的剩余时间内做 best-effort await，不重置预算。捕获 `InterruptedException` 后不立即 re-interrupt；当前 DrainRun/StopRun orchestration task 在自己线程内记住 `interrupted`，完成自己的 force/cleanup/result future 后在最外层 `finally` 恢复并退出。若 adapter 在独立 executor task 中运行，则由该 task 恢复自己的 flag，stop coordinator 只接收 cause/result，绝不替它恢复中断。
- `stopAcceptingNewRpcs()` 与 `force(ShutdownDeadline effectiveDeadline)` 均以 CAS 返回同一 future；direct TERM 传 StopRun deadline，普通 drain 才传 session 的 force budget，adapter 不得自行取 30/480 秒默认值。

### 9.3 Remoting 通用扩展点

**必须新增的服务端实现缝（复审 P1-7）。** 当前 `NettyRemotingServer` 没有持有完整的 child `ChannelGroup`，acceptor channel 也不是可供 drain 协调器使用的长期字段，因此下列各项属于**新增实现**而非已有能力：

- retained server channel（acceptor）作为长期字段；
- child channel tracking（完整 `ChannelGroup`）；
- 在 event loop 上按 `clientVersion × invocation mode`（sync/async/oneway）分支的 draining 标记与**可观测 barrier**；
- 对可安全响应的请求返回 `GO_AWAY` 而**不进入业务 dispatch**，并为 oneway / 旧版本客户端提供明确替代路径（见 §1.2）；
- freeze/close 的 deadline 行为。

**hot-path 预算必须有数据（复审 P1-7）。** `remoting` 是 Broker 最热路径，本方案新增 listener 调用、`RequestTaskContext` 字段与 scoped context 安装/清理。NOOP 默认实现只保证**行为**不变，不保证**性能**不变。Task 6 出口条件必须包含：每次 send 的 gate/acquire 原子操作、ChannelGroup 遍历、并发关闭与 5 MiB 请求的基准数据；Broker send 路径 p99 回归 ≤ 1%；结果写入 `findings.md` 作为提交证据。**不得**在没有数据时断言上游 hot path 一定不能接受，也不得在没有数据时宣称开销可忽略。

`remoting` 模块新增的 API 不包含 Proxy 类型：

```java
public interface RequestAdmissionAware {
    void setRequestLifecycleListener(RemotingRequestLifecycleListener listener);
}

public interface RequestIntakeController {
    CompletableFuture<Void> freezeRequestIntake();
    CompletableFuture<Void> closeBusinessChannels();
    int activeBusinessChannelCount();
}

public interface RemotingRequestLifecycleListener {
    RequestAdmissionResult beforeEnqueue(Channel channel, RemotingCommand request);
    default void onChannelActive(Channel channel) {}
    default void onChannelInactive(Channel channel) {}
}

public interface RequestTaskLifecycle {
    default void taskStarted() {}
    default void taskSkipped(TaskSkipReason reason) {}
    default void responseWriteStarted() {}
    default void responseWriteTerminal(boolean success, Throwable cause) {}
    default void channelClosed() {}
}
```

`RequestAdmissionResult` 有 `accepted(context)` 和 `rejected(code, remark, closeAfterWrite, context)` 两个 factory；即使拒绝没有 permit，也可携带 tracking-only context 统计 GO_AWAY/错误响应的 pending write。默认 listener 永远返回 accepted NOOP，确保 Broker/NameServer 和 feature-off 行为不变。

`RequestTaskContext` 直接持有 `RequestTaskLifecycle`。`RequestTask.run()` 只在执行线程把它安装到严格 finally 清理的 `ThreadLocal` scope；`AbstractRemotingActivity.processRequest` 在同步入口立即将具体 `RemotingSendLifecycle` 复制到 `ProxyContext`，异步 callback 不再访问 ThreadLocal。每个 accepted context 自己在 `channel.closeFuture()` 注册 once-only callback，以覆盖 before-dispatch/during-Broker close；这不是 channel+opaque side map。禁止把 context 放进 channel attribute 后按 opaque 查找。

`RequestTask` 将 `volatile stopRun` 改为原子 `CREATED/RUNNING/STOPPED`：

- `run()` 只有 `CREATED -> RUNNING` 的 CAS 获胜才安装 scope 并调用 runnable。
- CAS 获胜后先调用 `taskStarted()`，再进入 scoped runnable；`taskStarted` 只表示 executor dispatch，不得提前写 Broker STARTED。
- `setStopRun(true)`/新 `stop(reason)` 只有 `CREATED -> STOPPED` 的 CAS 获胜才写 `taskSkipped`。
- `returnResponse` 通过 callback-aware writer 调用 `responseWriteStarted/Terminal`，同步异常也 terminal。
- `shutdownNow()` 返回的 `FutureTaskExt` 解包后必须逐个 `stop(SHUTDOWN_NOW)`，不能只丢队列。

`NettyRemotingAbstract.processRequestCommand` 的固定实现顺序：

1. processor lookup；无 processor 按原逻辑返回。
2. 已处于全局 shutdown 时保持现有版本化 GO_AWAY。
3. 调用 optional lifecycle listener 分类 lease/gate，并得到 admission result/context。
4. 若 lifecycle 拒绝，使用带 callback 的 tracked writer 写 response，按结果决定 close；不构造 task。
5. 调用 processor `rejectRequest()`；若拒绝，对已获取 context 写 task skipped + tracked SYSTEM_BUSY。
6. 构造带 context 的 `RequestTask` 并 submit。
7. submit reject/其他异常写 task skipped，并跟踪 SYSTEM_BUSY/close。

`buildProcessRequestHandler` 捕获同一个 task context，使同步 response、AbortProcessException 和 SYSTEM_ERROR 都走 callback-aware writer。现有 `writeResponse(..., null)` 的调用点逐个替换；oneway/non-response 路径也显式 terminal NOOP context。

`NettyRemotingAbstract` 实现 `RequestAdmissionAware` 并保存默认 NOOP listener；`NettyRemotingServer`/`SubRemotingServer` 实现 `RequestIntakeController`。主 Server 保存目前只存在于 `start()` 局部变量中的 acceptor channel，并用 `ChannelGroup` 跟踪 child channel。主 Server 的 freeze/close 同时覆盖 `remotingServerTable` 内所有 subserver。

生命周期 child handler 的安装点必须在 `NettyRemotingServer.start()` 创建的**基类 `ChannelInitializer.initChannel`** 内，并且早于虚调用 `configChannel(ch)`：先加入专用 `RemotingLifecycleChannelHandler`，再让 normal 或 `MultiProtocolRemotingServer` 配置其协议 pipeline。不能把 handler 放进基类 `configChannel`（子类会覆盖），也不能只依赖异步 `ChannelEventListener`。handler 负责 `ChannelGroup` add/remove、`DRAIN_FROZEN` attribute、closeFuture 与 `onChannelActive/onChannelInactive`；协议协商后动态插入的 gRPC/Remoting handlers 不影响它。测试必须分别启动普通 `NettyRemotingServer` 与默认 `enableRemotingLocalProxyGrpc=true` 的 `MultiProtocolRemotingServer`，证明两者都被跟踪、freeze 后不再 autoRead，且 close future 只通知一次。

`freezeRequestIntake()`：

1. 先 CAS 设置 server-level `intakeFrozen`，再关闭并 await acceptor；与关闭竞态的 `channelActive` 看到该位后立即设置 channel freeze，确保不会漏出 snapshot。
2. 在每个 child 的 event loop 提交 barrier task，设置 `DRAIN_FROZEN=true` 与 `autoRead=false`。
3. `CompletableFuture.allOf` 完成表示该 event loop 上此前已排队 decode/dispatch 已越过 barrier。
4. `channelWritabilityChanged` 和其他恢复 read 的路径先检查 `DRAIN_FROZEN`，冻结后不得重新打开。
5. `newRemotingServer(port)` 创建 subserver 时复制 listener；否则辅助端口会绕过 drain。

`closeBusinessChannels()` 在 accepted send 和 pending write 归零后于各 event loop 关闭 Channel，并等待 `ChannelGroup` empty；它不代替 freeze barrier，也不批量释放 permit。

### 9.4 Proxy Remoting send 实现

`RemotingSendLifecycleListener` 只分类：

```text
RequestCode.SEND_MESSAGE
RequestCode.SEND_MESSAGE_V2
RequestCode.SEND_BATCH_MESSAGE
```

#### 事务消息亲和性：必须在编码前四选一（复审 P0-3）

「只豁免事务 Producer 的 lease」**不可实施**，不得再写成推荐实现。已确认事实：

- `ClusterTransactionService` 只在本 Proxy 的 `ProducerManager` 仍有本地 producer group 时向 Broker 维持心跳；
- Broker 的 transaction check 回到注册的 Proxy 后，`ProxyClientRemotingProcessor` 仍依赖该 Proxy **本地** producer channel；
- grpc-java `maxConnectionAge` 是 **Server 全局**配置，不知道 transport 后续会承载哪些 producer group；
- 同一 transport 可复用多种请求，当前没有事务专用 listener 或 SDK 变更。

必须选定并记录一种：

1. 全局关闭或显著拉长 steady-state gRPC lease，并重新定义扩容重平衡 SLO（与 §3.3 的压测结论联动）；
2. 引入事务专用 listener/路由或其他明确的亲和性边界；
3. 首期明确排除事务消息，并用生产流量 inventory 作为发布门禁；
4. 修改客户端/注册协议，使 transaction check 不再依赖原 Proxy 的本地 channel。

`CONSUMER_SEND_MSG_BACK`、`END_TRANSACTION`、`RECALL_MESSAGE` 和 oneway 不进入严格 gate。每个 active Channel 在 `onChannelActive` 写 270–330 秒的 monotonic lease deadline、expired flag 和 per-channel admitted-send count；这些都是单 Channel attributes，不含 opaque/request side map。随机源可注入并做边界测试。listener 在该 Channel 的 event loop 安排一次 expiry task：若届时 admitted count 为零则关闭 idle Channel，否则只写 expired flag；每个 permit 的 `completionFuture()` 将 admitted count 减至零后关闭已过期 Channel。lbCutoff 后新建的 Remoting Channel 在 `onChannelActive` 立即标记 late 并关闭；若 request 与 close 竞态，`beforeEnqueue` 仍按版本返回 GO_AWAY/legacy retry error，绝不进入 Broker。

在 producer acknowledged request 的 `beforeEnqueue`：

- lease 未到且 gate 开放：返回带 `RemotingSendLifecycle(permit)` 的 accepted。
- lease 到期且 client version `> V5_3_1`：返回 GO_AWAY、write 后保持/关闭策略按现有客户端测试确定，业务 processor 不执行。
- DRAINING 或 lbCutoff 后的 5.3.2+：返回 GO_AWAY 并阻止业务。
- legacy：返回 `SYSTEM_BUSY`，remark 固定为 `[PROXY_DRAINING] retry another proxy`，write terminal 后关闭 channel并增加 degraded 指标。
- channel 在 dispatch 前关闭：listener 调用 `tryBackendSkipped(CHANNEL_CLOSED_BEFORE_DISPATCH)` 并写 protocol terminal；随后 task 的 `backendStarted()` 必须失败。

`AbstractRemotingActivity.request(...)` 从 `ProxyContext` 取 lifecycle：

1. 先完成 brokerName/header 等所有同步解析；紧挨 `messagingProcessor.request/requestOneway` 前才调用 `backendStarted()`，false 立即返回且禁止 Broker request。
2. `messagingProcessor.request(...)` 同步抛错时 backend terminal 后写错误 response。
3. Future 完成时先 backend terminal，再进入 writer。
4. writer 在调用 `ctx.writeAndFlush` 前增加 pending；正常、write failure、同步 throw、inactive channel 都完成 protocol terminal并减少 pending。
5. `isWritable=false` 不再直接 return；active channel 仍尝试 write 并观察 Future，失败后 close 促使客户端重试。

`processRequest` 的 pipeline/校验异常，以及 `processRequest0` 在 Broker dispatch 前返回本地错误 response 的路径，显式执行 `tryBackendSkipped(SYNC_VALIDATION)` 后再写 response。writer 自身不得猜 backend 是否启动；如果该调用返回 false 且 `request(...)` 已经 STARTED，则同步 throw 分支必须先写 backend terminal，避免 outer catch 把它误判成 SKIPPED。

early reject 和 activity response 最终都调用同一个 request-scoped `RequestTaskLifecycle.responseWrite*` 契约。`RemotingDrainAdapter` 先关 gate、完成 event-loop barrier，再等 accepted sends/pending writes 归零，随后调用 `closeBusinessChannels()` 并等 active business channel 为零才报告 terminated；deadline 到达则关闭 channel、shutdownNow executor、记录 dropped task 并返回 forced。`freezeRequestIntake()` 与 `force(ShutdownDeadline effectiveDeadline)` 均 once-only/幂等；direct TERM 必须透传 StopRun deadline，不能回退使用 DrainSession 的 PreStop deadline。

## 10. 管理面、配置与可观测实现

### 10.1 `ProxyAdminServer`

使用 JDK 8 自带 `com.sun.net.httpserver.HttpServer`，不新增 Web 框架依赖。server 拥有固定 2 线程、有界队列 executor；`ProxyRuntime` 最后关闭并 await。每个 handler 统一设置 `Content-Type: application/json; charset=utf-8`、`Cache-Control: no-store`，限制 query 长度并拒绝 request body。

health response 最小 schema：

```json
{
  "status": "UP|DOWN",
  "state": "READY",
  "reason": "",
  "lifecycleEnabled": true,
  "drainId": null
}
```

`/state` 额外返回 startedAt、全部固定 cutoff、当前 phase、readiness contributors、gate closed/count、gRPC open send RPC/transport、Remoting pending write/channel、late/legacy/forced 计数。不得返回 client id、topic、地址明细或异常堆栈。

HTTP 语义固定为：

| 请求 | 成功 | 失败 |
|---|---|---|
| `GET /started` | 本地初始化完成 200 | 未完成/启动 fatal 503 |
| `GET /live` | STARTING 至 STOPPING 均 200 | fatal 或 STOPPED 503 |
| `GET /ready` | lifecycle 允许成员资格、listener 已绑定、无 fatal 时 200（共享依赖 fail-open） | 其余 503 |
| `GET /ready-for-traffic` | warmup 完成、listener 已绑定、**依赖健康**、无 fatal 时 200 | 依赖 fail-close、STARTING、STOPPED 均 503 |
| `GET /state` | loopback 200 | 非 loopback 403 |
| `POST /drain` | 首次或重入均 **202**，返回同一 drainId/cutoffs | lifecycle off 409；非 loopback 403；参数非法 400 |
| `GET /drain/{runId}` | 200，返回 phase/terminal/forced/剩余时间 | 未知 runId 404；非 loopback 403 |

方法错误返回 405 并写 `Allow`；未知 path 404。loopback 判断只使用 `HttpExchange.getRemoteAddress().getAddress().isLoopbackAddress()`，忽略所有 forwarded header。

**没有任何 handler 可以阻塞（复审 P1-4）。** `?wait=true&waitTimeoutSeconds=N` 与 504 语义整体删除：admin server 固定两线程，两个并发/重入 waiter 即可让 `/live`、`/ready` 无线程可用，导致 Pod 在 drain 中被 kubelet 杀死；JDK `HttpServer` 只有 server 级 `setExecutor()`，无法按 `HttpContext` 预留线程。等待改由调用方轮询 `GET /drain/{runId}` 完成。必须设置有界队列、并发上限与单请求超时，并把「`POST /drain` flood 下 `/live` 最大延迟」列为门禁；admin executor 自身服从 `StopDeadline`。

### 10.2 配置校验落点

`ProxyConfig.validateGracefulLifecycle()` 返回/抛出一条包含全部字段错误的异常，避免运维逐次修一个字段。`ProxyStartup.initConfiguration()` 在 `setConfigFromCommandLineArgument(...)` 后调用它，再打印最终配置；不能放在 `Configuration.init()`，因为后者早于 CLI override。

Java 校验至少覆盖：

- lifecycle=true 蕴含 admin=true、Cluster mode、两个 lease 开关为 true。
- admin port 范围合法，且不等于 gRPC、Remoting、metrics port。
- lease/grace/jitter/lb/send/PreStop/JVM/warmup 范围满足第 3.5 节。
- `ceil(lease * (1 + jitter))` 不溢出，fixed cutoff 严格单调，且 `hardDeadline <= preStopDeadline`。
- legacy policy 只能是 `SERVE_UNTIL_CUTOFF`。
- warmup topic trim 后非空、去重，禁止系统保留空字符串。

Helm 才能知道的 replicas/HPA/PDB/Pod/NLB 关系放 `values.schema.json` 与 template `fail`，不要在 Java 猜 Kubernetes 环境。

### 10.3 Metrics wiring

`ProxyLifecycleMetrics` 是每 runtime 一个实例，内部只保存 AtomicLong/LongAdder 与 instrument handles；exporter disabled 时使用同一数据对象但不注册 Meter，admin snapshot 仍可读。`ProxyMetricsManager.initClusterMode` 接收该实例，在 Meter 创建后调用 `lifecycleMetrics.bind(meter)`；重复 bind 失败。

gauge 读取 coordinator/gate/adapter snapshot，counter 只在状态事件发生时递增。label 枚举集中在 `ProxyMetricsConstant`，协议只允许 `grpc|remoting`，reason 使用代码枚举，禁止直接使用异常 message。metrics manager 必须晚于所有 forced/terminal event 关闭，保证最后一次 scrape/export 可见。`metricCollectorMode` 控制的是客户端指标收集，不得影响这些 Server lifecycle instruments；strict Helm profile 要求现有 `metricsExporterType=PROM`/5557 或等价可查询 exporter。

## 11. TDD 实施任务与提交边界

所有新测试继续由现有 JUnit 4/Surefire 2.19.1 执行，同时按仓库要求添加 `org.junit.jupiter.api.DisplayName`。必须明确：旧 runner 不展示该 annotation，它只作为源码元数据；真正的测试报告可读性由描述性的 JUnit 4 method name 保证。Task 1 首先用 test-compile 验证 parent 中 `mockito-junit-jupiter` 带来的 `junit-jupiter-api` 可用；若依赖树/编译证明不可用，才在 `proxy/pom.xml`、`remoting/pom.xml` 显式增加同版本的 `junit-jupiter-api` test dependency，绝不引入 Jupiter engine 或迁移现有测试。每个任务先提交失败测试或至少先运行并记录 RED，再写实现。下面命令中的单测类尚不存在时，预期 RED 为编译/测试失败；实现后必须 GREEN。

### Task 1：配置与绝对时钟 value object

**文件：**

- 修改 `proxy/src/main/java/org/apache/rocketmq/proxy/config/ProxyConfig.java`
- 修改 `proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java`
- 新增 `proxy/src/main/java/org/apache/rocketmq/proxy/lifecycle/{ProxyLifecycleState,DrainPhase,DrainTrigger,DrainSession,DrainRun,DrainResult,StopRun,StopResult,ShutdownDeadline}.java`
- 新增 `proxy/src/test/java/org/apache/rocketmq/proxy/lifecycle/DrainSessionTest.java`
- 新增 `proxy/src/test/java/org/apache/rocketmq/proxy/config/ProxyGracefulLifecycleConfigTest.java`

**步骤：** 先 test-compile 锁定 `@DisplayName` 只作 metadata 且不改变 JUnit 4 runner；再测默认 feature-off、CLI 后校验、`proxyPreStopWaitSeconds` 与全部非法范围聚合、T0/四个 cutoff 固定、drain hard deadline 与 PreStop/stop cap 分离、重复调用不能延长、nanoTime 跨界；再实现字段/validator/value objects。

```bash
rtk mvn -pl proxy -am -Dtest=DrainSessionTest,ProxyGracefulLifecycleConfigTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**提交：** `feat(proxy): define graceful lifecycle config and drain deadlines`

### Task 2：CAS gate 与 coordinator

**文件：** 新增 `ProxyLifecycle.java`、`ProxyLifecycleSnapshot.java`、`SendProtocol.java`、`SendLifecycleContext.java`、`SkipReason.java`、`ProtocolResult.java`、`SendDrainGate.java`、`SendPermit.java`、`ProxyLifecycleCoordinator.java`；新增对应 `SendDrainGateTest.java`、`ProxyLifecycleCoordinatorTest.java`。

**步骤：** 先覆盖数千线程 acquire/close 竞态、两种终态顺序、重复 callback、`tryBackendSkipped` 与 backend-start 正常竞态、真正非法 backend-terminal 转换、单个 `AtomicReference<DrainRun>` 不可观察 torn pair、重入 drain 返回同一对象、forced 互斥且 coordinator 停在 FORCE_DRAINING；用 fake scheduler 证明 direct TERM 的首个 tick 取消 60/420 秒 timer、gate 已关闭、两个 freeze 已发出，force adapter 只能看到 stop deadline。再实现 packed CAS 和单线程 orchestration。测试不得用真实 450 秒 sleep，使用 fake clock/手动 future。

```bash
rtk mvn -pl proxy -am -Dtest=SendDrainGateTest,ProxyLifecycleCoordinatorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**提交：** `feat(proxy): add linearizable send drain coordinator`

### Task 3：readiness、admin 与 runtime ownership

**文件：** 新增 `ReadinessBarrier.java`、`ReadinessContributor.java`、`ReadinessResult.java`、`FailureScope.java`、`ConstructionScope.java`、`ProxyRuntime.java`、`ProxyRuntimeFactory.java`、`lifecycle/admin/*`；修改 `ProxyStartup.java`；新增 `ReadinessBarrierTest.java`、`ProxyAdminServerTest.java`、`ProxyRuntimeTest.java`，扩展 `ProxyStartupTest.java`。

**步骤：** 先测 admin 先 bind 但 started/ready 失败、warmup 成功/恢复、READY 后 shared fail-open/local fail-close、真实 socket peer 限制、forwarded header 无效、重复 HTTP drain 同一 `DrainRun` 且 admin 保持存活、TERM 使用独立同一 `StopRun` 并由 hook `join()`、direct TERM 整体不超过 30 秒、`stopDeadline` 两个上限、顶层及复合 owner 内第 N 个 leaf 构造失败都逆序 rollback、部分启动失败关闭、feature-off Local `BrokerController` 先登记/先启后停兼容顺序；再删除静态 runtime 所有权并装配实例。

```bash
rtk mvn -pl proxy -am -Dtest=ReadinessBarrierTest,ProxyAdminServerTest,ProxyRuntimeTest,ProxyStartupTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**提交：** `refactor(proxy): own components in drain-aware runtime`

### Task 4：gRPC API spike 与 transport lifecycle

**文件：** 修改 `GrpcServerBuilder.java`、`GrpcServer.java`；新增 `lifecycle/grpc/*`；新增 `GrpcServerBuilderLifecycleTest.java`、`GrpcSendLifecycleTest.java`、`GrpcActiveCallRegistryTest.java`、`GrpcDrainStatusPolicyTest.java`、`GrpcTransportLifecycleFilterTest.java`、`GrpcServerShutdownTest.java`、`GrpcServerGoAwayIntegrationTest.java`，以及测试专用 `proxy/src/test/java/org/apache/rocketmq/proxy/grpc/GrpcRawHttp2GoAwayClient.java`。

**步骤：**

1. 先用 grpc-java 1.53.0 的真实 Netty server 锁定 `Server.shutdown()` 语义。`GrpcRawHttp2GoAwayClient` 必须是不经 gRPC Client API 的独立 raw shaded-Netty HTTP/2 客户端：父 pipeline 安装 `Http2FrameCodecBuilder.forClient().autoAckPingFrame(false).build()` 与 `Http2MultiplexHandler`，完成 connection preface/SETTINGS 后用 `Http2StreamChannelBootstrap` 打开 child stream，向测试 bidi/streaming method 发送包含 `:method=POST`、`:scheme=http`、`:path`、`:authority`、`content-type=application/grpc`、`te=trailers` 的有效 HEADERS（`endStream=false`）。raw client 从 `Http2FrameStream.id()` 记录本次 stream id，测试 service 只用 server-side latch 确认对应 handler 已启动，两者合起来证明该 id 已被服务端接受；保持流存活后才调用 `Server.shutdown()`，不假设 gRPC service Attributes 能暴露 HTTP/2 stream id。父 channel handler 按线序记录实际 `Http2GoAwayFrame`/`Http2PingFrame`：断言第一帧 `lastStreamId=Integer.MAX_VALUE,errorCode=NO_ERROR`，收到 payload `0x97ACEF001` 的非 ACK PING 后恰好一次回写 `new DefaultHttp2PingFrame(payload, true)`，再断言第二帧 `lastStreamId` 等于已接受的最高 stream id 且二者均为 `NO_ERROR`，同时断言客户端只发出一帧 ACK PING，排除 Netty auto-ACK 和手工 ACK 重复。同一测试证明 `Server.shutdown()` 非阻塞、现存 stream 不被立即取消、`awaitTermination` 只等待、`shutdownNow` 才强制取消。断言只基于捕获的真实帧和 Future，不匹配日志字符串；生产代码和测试 helper 都不得引用 grpc-java package-private 类。
2. 用编译测试锁定 builder API、tracer-before-interceptor 顺序、多个 interceptor 的实际执行顺序和 filter Attributes，并证明现有 `ContextInterceptor` 从 `Context.current()` 派生后仍保留 tracer holder；测试明确证明 transport filter 不提供单 transport close。
3. 先写 `GrpcActiveCallRegistryTest`：覆盖初始 count=0 时 future 未完成、随后 register Telemetry 再 closeAll、数千次 register/closeAll 竞态、Telemetry 在第一条 SETTINGS 前已登记、所有非 unary descriptor 自动分类、正常 close/drain close/client cancel 三方并发、close intent 不减少计数、只有 listener complete/cancel terminal、`next.startCall` 同步抛错补偿，以及 registry 关闭入口后新 call 立即拒绝且不增加 open count。用会检测并发调用的 fake `ServerCall` 证明 `sendHeaders/sendMessage/closeForDrain` 已被同一 serial section 保护；`GrpcDrainStatusPolicyTest` 使用生成 descriptor 证明只有 Telemetry 得到 OK，ReceiveMessage/PullMessage 与一个未来 synthetic streaming method 均得到 retryable UNAVAILABLE。
4. 再实现 send holder CAS bind、active-call wrapper/registry、late rejection、open RPC/transport barrier和 `GrpcServer` 四段式接口。用 fake `Server` 断言 `initiateServerDrain`、`forceServerShutdown` 各自 once-only，strict 路径绝不读取固定 `grpcShutdownTimeSeconds`，`await=false`/interrupt 均转成明确 cause。为普通 DrainRun 的独立 adapter task 增加中断测试：被中断后 force 和该 task 负责的后续 close 仍会执行，result future 的 inline completion 时 interrupt flag 仍为 false，只在这个 adapter task 自己的最外层 `finally` 恢复后退出；断言没有让尚未存在的 StopRun 或其他线程代为恢复。
5. 用 fake EventLoopGroup 证明 builder/start 第 N 个 leaf 失败逆序回收；正常 drain 不注销 TLS listener、不关闭 groups；STOPPING 正常路径在 Server 终止后按同一 stop deadline 注销 listener 并关闭 worker/boss。再用一个 `shutdownNow()` 后仍不 terminated 的 stubborn fake Server 断言：第二次 await 失败会产生 `server_not_terminated`，TLS listener、worker、boss 均仍被尝试关闭且每个只一次，deadline 为零时只发起非阻塞 group shutdown，后续 runtime owner 仍继续。最后运行真实 Telemetry/ReceiveMessage 组件测试：send 完成后 registry 对 Telemetry 发出 OK completion、对 ReceiveMessage 发出 `UNAVAILABLE/[PROXY_DRAINING] reconnect`，listener terminal、transport terminated、Server terminated 依次成为可观察屏障，正常路径不调用 `shutdownNow()`；正式 5.0.7/5.2.1 的 1 秒 renewal 放到第 5.2 节隔离 JVM 客户端矩阵验收，不能把 fake observer 当成 SDK 证据。

```bash
rtk mvn -pl proxy -am -Dtest=GrpcServerBuilderLifecycleTest,GrpcSendLifecycleTest,GrpcActiveCallRegistryTest,GrpcDrainStatusPolicyTest,GrpcTransportLifecycleFilterTest,GrpcServerShutdownTest,GrpcServerGoAwayIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

拆成两个提交，确保协议证据与生产接线可独立审查：

1. `test(proxy): lock grpc graceful shutdown semantics`
2. `feat(proxy): own and drain grpc calls in phases`

### Task 5：gRPC application 双终态

**文件：** 修改 `ProxyContext.java`、`ContextVariable.java`、`GrpcMessagingApplication.java` 和 `GrpcMessagingApplicationTest.java`；使用 Task 2 已新增的 `lifecycle/SendLifecycleContext.java`。

**步骤：** 先测 context 未跨 executor 自动传播、queued runnable 才 backend-start、cancel-before-run 禁止 Broker、validation/execute/reject 为 SKIPPED、Future 正常/异常/同步 throw terminal、response completion 不替代 tracer terminal；必须构造容量为 1 的真实 `ThreadPoolExecutor`，占满 worker 与 queue 后调用 `addExecutor`，断言 reject handler 收到原始 `GrpcTask`、`tryBackendSkipped` 成功、Broker 零调用、streamClosed 后 permit/open RPC 都归零。再把 `submit` 改为 `execute` 并改 application，保留非 send RPC 行为。

```bash
rtk mvn -pl proxy -am -Dtest=GrpcMessagingApplicationTest,GrpcSendLifecycleTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**提交：** `feat(proxy): bind grpc broker futures to send permits`

### Task 6：Remoting 模块通用 task lifecycle

**文件：** 新增第 9.3 节 remoting API；修改 `RequestTask.java`、`NettyRemotingAbstract.java`、`NettyRemotingServer.java`；新增 `RequestTaskLifecycleTest.java`、`NettyRemotingServerLifecycleTest.java`，扩展 `NettyRemotingAbstractTest.java`。

**步骤：** 先测 NOOP 兼容、scope 在线程复用后清理、stop/run CAS、flow/submit reject、同步 write throw、Future success/failure、subserver 继承 listener、freeze event-loop barrier 与 autoRead fence；在 remoting 模块以普通 Server 和一个覆盖 `configChannel` 的测试子类证明 lifecycle handler 由基类 initializer 预先安装。再实现通用 seam。该提交不得引用 Proxy 模块的 `MultiProtocolRemotingServer` 或其他 Proxy 类型；真实默认 MultiProtocol 路径由 Task 7 锁定。

```bash
rtk mvn -pl remoting -Dtest=RequestTaskLifecycleTest,NettyRemotingAbstractTest,NettyRemotingServerLifecycleTest test
```

**提交：** `feat(remoting): expose request-scoped lifecycle hooks`

### Task 7：Proxy Remoting lease、gate 与 writer

**文件：** 新增 `lifecycle/remoting/*`；修改 `RemotingProtocolServer.java`、`AbstractRemotingActivity.java`、`SendMessageActivity.java`；新增 `RemotingSendLifecycleTest.java`、`RemotingDrainAdapterTest.java`、`MultiProtocolRemotingServerLifecycleTest.java`，扩展 `AbstractRemotingActivityTest.java`、`SendMessageActivityTest.java`。

**步骤：** 先测只分类三种 producer send、270/330 边界、5.3.2 GO_AWAY、legacy degrade、channel-before/during-backend、non-writable/write throw/failure、queue cleanup、barrier 后零 late dispatch；再以默认 `enableRemotingLocalProxyGrpc=true` 实例化真实 `MultiProtocolRemotingServer`，完成协议协商后断言 lifecycle child handler 仍跟踪、freeze、close。最后安装 listener 和 tracked writer。

```bash
rtk mvn -pl proxy -am -Dtest=RemotingSendLifecycleTest,RemotingDrainAdapterTest,MultiProtocolRemotingServerLifecycleTest,AbstractRemotingActivityTest,SendMessageActivityTest -Dsurefire.failIfNoSpecifiedTests=false test
```

**提交：** `feat(proxy): drain remoting sends with scoped permits`

### Task 8：有界资源关闭与 metrics

**文件：** 使用 Task 1 的 `ShutdownDeadline.java`，新增 `DeadlineAwareShutdown.java`、`ExecutorShutdown.java`、`ProxyLifecycleMetrics.java`；修改 `ProxyRuntime.java`、`GrpcServer.java`、`GrpcMessagingApplication.java`、`DefaultGrpcMessagingActivity.java`、`RemotingProtocolServer.java`、`DefaultMessagingProcessor.java`、`ClusterServiceManager.java`、`ClusterMetadataService.java`、`DefaultReceiptHandleManager.java`、`TopicRouteService.java`、`HeartbeatSyncer.java`、`ClusterTransactionService.java`、`GrpcChannelManager.java`、`ProxyMetricsManager.java`、`ProxyMetricsConstant.java`；新增 `ProxyExecutorShutdownTest.java`、`ProxyLifecycleMetricsTest.java` 并扩展 runtime/协议 shutdown 测试。Local mode 不开启 strict lifecycle，保留原兼容关闭入口。

**步骤：** 先用 owner 级测试证明上述每个 Cluster executor 在共享 stop deadline 内 terminated，且同一 deadline 不会被嵌套组件重置；证明 `RemotingProtocolServer`、`ClusterServiceManager`、`GrpcChannelManager` 构造/init 不再调度，只有 `start()` 后有任务，且 `DefaultGrpcMessagingActivity` 关闭 manager；再实现 deadline-aware overload。覆盖 `await=false`、interrupt、shutdownNow dropped tasks、ReceiptHandle 两个当前未注册 worker、`ThreadPoolMonitor.shutdown()` 恰好一次、metrics exporter disabled；额外断言 strict runtime/adapter 不持有原始 gRPC `Server`，DRAINED 后 STOPPING 只幂等确认 Server terminal 而不重新应用固定 timeout，TLS listener/EventLoopGroups 恰好回收一次。增加 stubborn Server 和已耗尽 deadline 用例，证明 `shutdownNow()` 后仍未终止时会记录 `server_not_terminated`，仍发起 worker/boss shutdown 并继续后续 owner；另外单独增加 StopRun 专用线程在 Server await 期间被中断的用例，断言 TLS listener、worker、boss 和后续 owner 均各尝试一次，过程中不因过早 re-interrupt 而全部短路，stop future 的 inline completion 回调观察到未中断，最外层 `finally` 恢复的是该 StopRun 线程自己的 flag，而不是 Task 4 中 DrainRun adapter 线程的 flag。最后验证 `grpcOpenDrainableCalls`、正常 non-send close、server-await timeout/force cause 和 `server_not_terminated` 均为低基数指标，metrics 在协议之后关闭。

```bash
rtk mvn -pl remoting,proxy -am -Dtest='*LifecycleTest,*ShutdownTest,*MetricsTest' -Dsurefire.failIfNoSpecifiedTests=false test
```

**提交：** `feat(proxy): bound shutdown and export drain evidence`

### Task 9：CLI、镜像与 PID 1 信号链

**文件：**

- 新增 `distribution/bin/mqproxyctl`
- 修改 `distribution/bin/mqproxy`
- 修改 `distribution/bin/runserver.sh`
- 修改 `docker/scripts/runserver-customize.sh`
- 修改 `docker/Dockerfile`
- 新增 `docker/tests/test-proxy-signal-chain.sh`
- 新增 `docker/tests/test-mqproxyctl.sh`

`mqproxy` 与两个 runner 的最终进程启动均使用 `exec`；entrypoint 已正确 `exec ./mqproxy`，不做无关改动。`mqproxyctl` 固定实现 `drain`、`status`、`version --output json`：前两者只访问 loopback admin，version 输出 image capability/build 元数据。测试用 fake Java/curl 断言 argv、退出码、PID 与 TERM 传递；drain 对 200/202 成功，409/503/504 非零，并保留 response body 供 PreStop 日志。

```bash
rtk bash docker/tests/test-proxy-signal-chain.sh
rtk bash docker/tests/test-mqproxyctl.sh
rtk docker build -f docker/Dockerfile .
```

**提交：** `fix(distribution): deliver proxy termination to the jvm`

Helm、升级脚本、provider spike 和最终矩阵的逐文件任务见下一节；它们与 Java 各自独立提交，避免一个回退同时撤销协议与运维护栏。

## 12. `/Users/lossend/pro/rocketmq-helm` 逐文件实现

Helm 仓库没有 Dockerfile、entrypoint 或 launcher；第 11 节 Task 9 产出的 Proxy 镜像是 Chart 的前置契约。Chart 不得用 ConfigMap shell wrapper 掩盖旧镜像缺少 `exec`/`mqproxyctl`/8082 的事实。

### 12.1 镜像 capability contract

Proxy 镜像必须同时满足：

- stock/custom runner 最终 `exec java`，TERM 到达 JVM。
- `mqproxyctl drain|status|version` 可执行；`version --output json` 至少输出 `apiVersion=v1`、admin/drain capability 和 build version。
- 包含第 3.5 节全部 ProxyConfig key，admin 可监听 8082。
- 镜像通过 signal-chain smoke test；Helm 升级脚本只消费结果，不在运行时 patch 镜像。

主 Chart 新增独立 `proxy.image.repository/tag/digest/pullPolicy`，为空时兼容回退 `global.image`；strict profile 禁止 `latest` 和未显式固定的镜像。`templates/_helpers.tpl` 新增 `rocketmq.proxy.image`，所有 Proxy workload 只调用该 helper。

### 12.2 values/schema/模板校验

**修改：**

- `/Users/lossend/pro/rocketmq-helm/values.yaml`
- `/Users/lossend/pro/rocketmq-helm/templates/_helpers.tpl`

**新增：**

- `/Users/lossend/pro/rocketmq-helm/values.schema.json`
- `/Users/lossend/pro/rocketmq-helm/templates/_validation.tpl`
- `/Users/lossend/pro/rocketmq-helm/tests/test_proxy_values_schema.py`

values 使用第 3.5 节已经冻结的 Helm key，不再另造一套 `connectionLease.min/max` 命名；其中 `preStopWaitSeconds` 也映射 Java 的 `proxyPreStopWaitSeconds`，只有 image/replica/PDB/HPA/safety/provider evidence 属于 Helm orchestration。完整结构为：

```yaml
proxy:
  image: {repository: "", tag: "", digest: "", pullPolicy: IfNotPresent}
  lifecycle:
    enabled: false
    strict: false
    admin: {enabled: false, bindAddress: 0.0.0.0, port: 8082}
    grpcLease: {enabled: true}
    remotingLease: {enabled: true}
    connectionLeaseSeconds: 300
    connectionLeaseGraceSeconds: 30
    remotingLeaseJitterRatio: 0.10
    lbDetachQuietSeconds: 20
    lbDetachTimeoutSeconds: 60
    sendDrainTimeoutSeconds: 30
    warmupTopics: []
    warmupTimeoutSeconds: 60
    jvmShutdownTimeoutSeconds: 30
    legacyRemotingPolicy: SERVE_UNTIL_CUTOFF
    preStopWaitSeconds: 480
    terminationSafetyMarginSeconds: 30
    providerHealthPortVerified: false
    dependencyFailureThreshold: 3
    dependencyWaitSeconds: 0
    readyForTrafficDuringDrain: false
  minReadySeconds: 0                 # feature-off 兼容默认；strict profile 显式设 600
  progressDeadlineSeconds: 600       # feature-off 兼容默认；strict profile 显式设 >=1800
  terminationGracePeriodSeconds: 120 # 当前兼容默认；strict profile 显式设 540
  pdb: {enabled: true, minAvailable: 1, maxUnavailable: null}
```

`proxy.autoscaling.*` 整组删除，`templates/proxy-hpa.yaml` 不再渲染（复审 P1-8）。

schema 校验类型/枚举/单值范围，`_validation.tpl` 聚合跨字段错误并由 `templates/proxy.yaml` 顶部调用。严格模式至少校验：

```text
lifecycle.enabled && lifecycle.admin.enabled
replicas 显式设置（不接受 HPA 托管）
remaining_ready_capacity >= peak_required_capacity * safetyFactor   # 见 §4.1 容量不等式
PDB minAvailable/maxUnavailable 二选一
lbDetachTimeout
  + ceil(connectionLeaseSeconds * (1 + remotingLeaseJitterRatio))
  + connectionLeaseGraceSeconds
  + sendDrainTimeoutSeconds
  <= preStopWaitSeconds
preStopWaitSeconds + jvmShutdownTimeoutSeconds
  + terminationSafetyMarginSeconds
  <= terminationGracePeriodSeconds
minReadySeconds >= terminationGracePeriodSeconds + 60
progressDeadlineSeconds >= 1800
不存在 proxy.autoscaling.* 字段（存在即 fail）
providerHealthPortVerified == true
stable remotingAccessAddr 非空
strict profile 的 warmupTopics 非空
proxy.config.metricsExporterType != DISABLE，且 rollout supervisor 可查询该 exporter
```

schema 不负责 provider 能力真实性；`providerHealthPortVerified` 只能由第 1 步真实 spike 产出的证据打开，升级 preflight 还要复查目标 annotations/target health。

### 12.3 主 Chart workload

**修改：**

- `/Users/lossend/pro/rocketmq-helm/templates/proxy.yaml`
- `/Users/lossend/pro/rocketmq-helm/templates/_helpers.tpl`
- `/Users/lossend/pro/rocketmq-helm/tests/test_workload_rollout_and_scheduling.py`

**新增：**

- `/Users/lossend/pro/rocketmq-helm/tests/test_proxy_graceful_lifecycle_chart.py`

`templates/proxy.yaml` 逐块修改：

1. ConfigMap 将 values 映射到第 3.5 节 Java key；该 ConfigMap 是时间参数唯一配置源并继续进入现有 checksum。
2. Deployment 始终渲染 `spec.replicas`；不渲染 HPA、不留 replicas ownership 分支。
3. 渲染 `minReadySeconds`、`progressDeadlineSeconds`、`terminationGracePeriodSeconds`，保持 `maxSurge: 1/maxUnavailable: 0`。
4. image 改用 `rocketmq.proxy.image`。
5. command/args 直接执行 `./mqproxy`；不再生成 `/bin/sh -ec './mqproxy ...'`。
6. 增加 container-only 命名端口 `admin: 8082`，但不加入 ClusterIP Service 或 NLB listener。
7. admin-only bootstrap：仍使用旧 TCP probes/`mqshutdown` PreStop，但镜像已提供兼容态 8082。
8. lifecycle enabled：startup/readiness/liveness 分别为 `/started`、`/ready`、`/live`；PreStop 为 `./mqproxyctl drain --wait --timeout 480s`，不加 `|| true`。
9. PDB 删除硬编码 `minAvailable: 1`，按 values 渲染且只允许一种策略。
10. 增加 `POD_NAME/POD_NAMESPACE` Downward API，供 drain 日志关联，不作为 metrics label。

无需修改 `templates/tools-configmap.yaml`：`mqproxyctl` 必须来自经过验证的镜像，不从 Chart 注入。

render 测试用 `yaml.safe_load_all` 断言 feature-off/admin-only/strict 三种 manifest、HTTP path/port、PreStop 退出码、admin 不进入 Service、600/540/1800、PDB values 生效、`spec.replicas` 恒定渲染且无 HPA 对象、独立 image fallback 和 ConfigMap key 一致。

### 12.4 NLB、NetworkPolicy 与 PrometheusRule

**删除：**

- `/Users/lossend/pro/rocketmq-helm/templates/proxy-hpa.yaml` 不新增（复审 P1-8）；若仓库已存在则删除，并由 §12.2 的负例校验保证 `proxy.autoscaling.*` 不再出现。

**修改：**

- `/Users/lossend/pro/rocketmq-helm/templates/proxy-nlb.yaml`
- `/Users/lossend/pro/rocketmq-helm/templates/networkpolicy.yaml`
- `/Users/lossend/pro/rocketmq-helm/templates/monitoring.yaml`
- `/Users/lossend/pro/rocketmq-helm/tests/test_proxy_graceful_lifecycle_chart.py`

NLB 保留 gRPC listener，可选渲染稳定 Remoting listener；health check 单独指向 Pod 8082 **`/ready-for-traffic`**，绝不创建业务 8082 listener。AWS/ACK values 显式锁定 target type、cross-zone、externalTrafficPolicy、health interval/threshold、450 秒 deregistration/connection drain，以及 §4.2 表格的两条连接终止属性——`deregistration_delay.connection_termination.enabled=false` 与 `target_health_state.unhealthy.connection_termination.enabled=false`（后者 AWS 默认为 `true`，必须显式覆盖）。provider 不支持独立 health port 时 strict validation 失败。

`networkpolicy.yaml` 修正当前错误 selector，Proxy policy 选择 `app.kubernetes.io/component=proxy`；业务、metrics、admin health 分端口/来源配置。即使 CIDR 放行 8082，远端 `/state`/POST 仍由应用拒绝。

`monitoring.yaml` 继续由现有 ServiceMonitor 抓 5557，不抓 admin。增加可选 PrometheusRule：deadline/forced/late/transport failure 任一增量报警，QUIESCING/DRAINING 超长报警，READY 副本不足报警。

### 12.5 Provider values

只修改 tracked provider values，不读取或覆盖用户的 `in.yaml`、`prod-euc.yaml`、`prod-in.yaml`、`test-in.yaml`：

- `values-testing-aws.yaml`
- `values-testing-alibaba.yaml`
- `values-production-aws-data.yaml`
- `values-production-ack-data.yaml`

测试环境先 pin 新 Proxy image，开启 admin/lifecycle 但 `strict=false`，验证 provider health port 和全部预算。生产 enablement 独立到最后提交：`strict=true`、固定镜像、540 秒 grace、PDB、稳定 `remotingAccessAddr`、provider drain annotations；不得与默认关闭的基础模板混成一个不可拆回退的提交。

### 12.6 Standalone 降级 Chart

**修改：**

- `proxy-sg-standalone/values.yaml`
- `proxy-sg-standalone/templates/configmap.yaml`
- `proxy-sg-standalone/templates/deployment.yaml`
- `proxy-sg-standalone/templates/service-nlb.yaml`
- `proxy-sg-standalone/templates/_helpers.tpl`
- `tests/test_proxy_standalone_chart.py`

**新增：**

- `proxy-sg-standalone/values.schema.json`
- `proxy-sg-standalone/templates/_validation.tpl`

Deployment 增加 direct command、admin container port、三种 HTTP probe、PreStop、termination grace、minReadySeconds 和显式 rolling strategy；ConfigMap 映射同一 Java keys；NLB 使用 provider HTTP health check。确认 `templates/service.yaml` 与 NLB 都不暴露 admin listener。默认单副本/`strict=false`，schema 明确禁止单副本开启 strict。

### 12.7 升级 supervisor

**新增：**

- `scripts/deploy/proxy_rollout.py`
- `scripts/tools/check-proxy-image-contract.py`
- `tests/test_proxy_rollout.py`
- `tests/test_proxy_image_contract.py`
- `tests/test_upgrade_proxy_lifecycle.py`

**修改：**

- `scripts/deploy/upgrade-prod.py`
- `scripts/deploy/upgrade-testing.py`
- `scripts/deploy/upgrade-proxy-standalone.py`
- `tests/test_upgrade_testing.py`
- `tests/test_script_context_paths.py`

`check-proxy-image-contract.py` 通过可 mock 的 subprocess adapter 验证 image 内 `mqproxyctl version --output json`、lifecycle API version 与 signal smoke-test attestation；失败时在 Helm 变更前退出。

`proxy_rollout.py` 集中实现：

1. `helm template/lint/schema` 并从 manifest 读取 image/预算/replica/PDB。
2. `kubectl exec <pod> -- ./mqproxyctl status` 从 loopback 读取状态；不远程访问 `/state`。
3. 拒绝存在 deletionTimestamp、非 READY、已有 drain、NLB target 不健康或 provider evidence 不符的集群。
4. **断言目标 workload 无 live HPA**（`kubectl get hpa` 按 scaleTargetRef 匹配）。检测到即 fail 并退出，不尝试冻结或接管——固定 replicas 是本方案「最多一个旧 Pod Terminating」串行 drain 约束的前提（复审 P1-8）。
5. **单次** upgrade：以目标 chart/values 执行不带 `--atomic` 的 `helm upgrade --wait`，timeout 下限按 replica/minReady/grace/启动余量计算。不再有 prepare/main/restore 三段事务与 frozen overlay。
6. 持续断言最多一个旧 Pod Terminating、下一 Pod 删除前上一 Pod 已 DRAINED/消失；违约立即 `kubectl rollout pause` 并停止后续动作。
7. rollout 判据以 §3.7 选定的持久 drain result 交接为主、指标为辅；forced 按 §4.4 的分级策略处理（`accepted_sends > 0` pause，`== 0` warn 继续）。send SLO 或 NLB 异常同样 pause，不自动反向 rollout。交接读写失败、missing/stale series、query timeout 一律 fail-closed。
8. manual scale-down 每次只改 1 replica，join 当前 drain/删除后才进行下一次。

supervisor 为每次事务保存 current revision、target values hash 与 upgrade revision；重入时依据 release revision 恢复而非猜测阶段。三个现有 upgrade 脚本只保留环境解析/确认并调用共享 supervisor，不能各自复制一版 drain 逻辑。controlled rollback 是 supervisor 独立子命令，复用同一单 Pod gate。

## 13. Helm/发布 TDD 任务与提交边界

### Task 10：values、schema 与 image contract

先新增 `test_proxy_values_schema.py` 和 `test_proxy_image_contract.py`，覆盖默认兼容、所有预算反例、严格副本/HPA/PDB、global image fallback、latest/旧 capability 拒绝；再实现 schema/helper/validation/checker。

```bash
cd /Users/lossend/pro/rocketmq-helm
rtk pytest -q tests/test_proxy_values_schema.py tests/test_proxy_image_contract.py
rtk helm lint .
```

**提交：** `feat(chart): add proxy lifecycle values and validation`

### Task 11：主 Chart 生命周期与运维护栏

先写 lifecycle off/admin-only/strict render 测试，再修改 proxy/PDB/HPA/NLB/NetworkPolicy/monitoring；不改生产 values。

```bash
rtk pytest -q tests/test_proxy_graceful_lifecycle_chart.py tests/test_workload_rollout_and_scheduling.py
rtk helm template rocketmq-data . --namespace rocketmq
```

拆成两个提交：

1. `feat(chart): wire proxy graceful lifecycle`
2. `feat(chart): add proxy nlb drain guards`

### Task 12：Standalone 降级接入

先让测试解析完整 YAML 并断言 admin 不暴露、HTTP probes/PreStop/grace/单副本 strict reject，再改 standalone 文件。

```bash
rtk pytest -q tests/test_proxy_standalone_chart.py
rtk helm lint proxy-sg-standalone \
  --set-string namesrvAddr=nameserver.example:9876 \
  --set-string clusterName=test-cluster \
  --set acl.enabled=false
```

**提交：** `feat(standalone): add degraded proxy lifecycle support`

### Task 13：rollout supervisor

所有 kubectl/helm/docker/metrics 调用先做 adapter + fake 输出测试；覆盖 **检测到 live HPA 即 fail** 的负例、单次 upgrade 路径、pause-on-second-terminating、脚本崩溃按 revision 恢复、timeout 公式、无 `--atomic`、受控 rollback 和逐 1 缩容。另需覆盖 drain result 交接：读写失败/missing/stale/query timeout 全部 fail-closed，以及 forced 分级（`accepted_sends > 0` pause、`== 0` warn 继续）。

```bash
rtk pytest -q tests/test_proxy_rollout.py tests/test_upgrade_proxy_lifecycle.py \
  tests/test_upgrade_testing.py tests/test_script_context_paths.py
```

**提交：** `feat(deploy): supervise proxy drain-aware rollouts`

### Task 14：provider spike、测试 values 与生产 enablement

先在 AWS/ACK 隔离 release 证明：8082 不作为 listener 仍可作 target health port；readiness 失败至最后新连接 p999 ≤60 秒；450 秒 deregistration 与 Proxy 时钟并行。证据未通过时不得提交 `providerHealthPortVerified=true`。

先在 AWS/ACK 隔离 release 还必须证明 §4.2 表格的两条连接终止属性实际生效：deregistration draining 期间既有连接不被 RST；target 被判 unhealthy 时既有连接不被 RST。另需实测 draining 期间健康检查是否仍在评估、评估出 unhealthy 后是否仍终止连接——该行为文档未定义，只能抓包确认，结论直接决定 §8.4 的 `/ready-for-traffic` 是否可用条件式 200。

测试 values 独立提交：`test(chart): exercise proxy lifecycle on provider test profiles`。完成 canary 与 annotation bootstrap 后，生产 values/Chart version 单独提交：`chore(prod): enable graceful proxy lifecycle`，确保一键回退只撤生产 enablement，不撤底层能力。

### Task 15：全量验证与格式化

```bash
cd /Users/lossend/opensource/rocketmq
rtk mvn -pl remoting,proxy -am test
rtk mvn clean compile
rtk ur-format
rtk bash docker/tests/test-proxy-signal-chain.sh
rtk bash docker/tests/test-mqproxyctl.sh

cd /Users/lossend/pro/rocketmq-helm
rtk pytest -q
rtk helm lint . -f values-production-aws-data.yaml
rtk helm lint . -f values-production-ack-data.yaml
rtk helm template rocketmq-data . --namespace rocketmq -f values-production-aws-data.yaml
rtk helm template rocketmq-data . --namespace rocketmq -f values-production-ack-data.yaml
rtk git diff --check
```

随后执行第 5.2 节客户端/provider 矩阵。Java/Chart 单测、kind 和 template 只能作为前置门，不能替代真实 NLB、持续 send 与单 Pod 删除证据。
