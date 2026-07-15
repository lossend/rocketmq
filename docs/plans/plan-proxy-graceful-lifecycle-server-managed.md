# RocketMQ Proxy 无 SDK 改造的优雅上下线重设计

> **实施要求：** 使用 `planning-with-files` 与 `executing-plans` skills 按阶段执行；任何实现开始前先重新读取本计划、`task_plan.md`、`findings.md` 和 `progress.md`。

> **参考关系与优先级：** 本文是 Server/Helm-only 方案的规范性计划；原文档 [`plan-proxy-graceful-lifecycle.md`](./plan-proxy-graceful-lifecycle.md) 仅作为 Proxy 实现参考。可复用其中生命周期协调器、CAS 准入门、双终态计数、资源逆序关闭、指标与全链路 `exec` 的思路；不得复用 SDK 双连接、`proxyInstanceId`、Drain Notice/ACK、新 Proto 或 120 秒时钟。两份文档冲突时一律以本文为准，原文档保持不改。

**目标：** 仅修改 RocketMQ Proxy Server 与 `/Users/lossend/pro/rocketmq-helm`，让计划内扩容、滚动重启和受控缩容对 acknowledged send 达到零观测错误/超时，并限制尾延迟增量。

**架构：** 使用稳定 L4 Service/NLB、5 分钟随机连接租约、统一 Proxy 生命周期状态机和 Kubernetes 长排空窗口。客户端不增加双连接、不增加 Drain Proto/ACK，也不引入 Gateway。

**技术栈：** Java 8、gRPC Java/Netty、RocketMQ Remoting、Kubernetes Deployment/Service/HPA/PDB、Helm、AWS/ACK NLB、OpenTelemetry/Prometheus。

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
- `GrpcServer.shutdown()` 忽略等待超时结果，执行器 shutdown 后不 await。
- Helm 使用 TCP 探针和立即 `mqshutdown` 的 PreStop；NLB 摘流、连接迁移和 JVM 终止窗口没有统一时钟。
- 容器启动链存在未 `exec` 的 shell，SIGTERM 不保证抵达 JVM。
- 主 Chart 的生产 PDB 值被模板硬编码为 1；没有 HPA 缩容节奏，Remoting 稳定访问地址也未强制配置。
- L4 长连接不会因为扩容自动迁移，新 Pod 只能承接新连接。

严格承诺仅覆盖：

- Kubernetes 计划内 rollout、restart、HPA 或脚本控制的逐 Pod scale-down。
- 有响应的同步/异步 send；`sendOneway` 不在严格范围内。
- RocketMQ 原有 at-least-once 语义；重连竞态产生的重复消息不算方案失败，消费者仍需幂等。

不承诺 SIGKILL、OOM、节点丢失、网络分区、Pod IP 直连或一次删除多个 Pod。

## 2. 客户端版本分析与兼容契约

| 客户端 | 等级 | 已验证行为与设计含义 |
|---|---|---|
| `rocketmq-client-java:5.0.7` | 严格验收 | gRPC Java 1.50；每个 Endpoints 一个 ManagedChannel；`pick_first`；关闭 gRPC 自动重试；Producer 默认 3 次尝试、单次请求默认 3 秒。标准 GO_AWAY 会重建 transport，竞态失败由 Producer 重试吸收。 |
| `rocketmq-client-java:5.2.1` | 严格验收 | send 迁移能力与 5.0.7相同；新增 300 秒 keepalive。`ReconnectEndpointsCommand` 只设置布尔标志，不关闭或重建 Channel，因此不能作为迁移机制。 |
| `rocketmq-client:5.3.2+` | 严格验收 | Remoting 支持 GO_AWAY 后重连并透明重试；服务端现有版本门槛为 `> V5_3_1`。 |
| `rocketmq-client:5.2.0` 及更早 | 降级兼容 | 5.2.0 虽有重连代码，但当前服务端不会向其发送 GO_AWAY；只能在最终断链后依赖普通重连/Producer 重试。 |
| 经典 Remoting 5.2.1 | 不存在 | 官方 5.2.1 坐标属于 `rocketmq-client-java`；经典 `rocketmq-client` 没有 5.2.1 发布。 |

严格测试使用客户端默认或更高的 `maxAttempts=3`、至少 3 秒请求超时，并保持 Remoting `enableReconnectForGoAway=true`。不修改任何 SDK 源码或客户端协议。

## 3. Proxy 运行时重构

### 3.1 Runtime 所有权、生命周期与就绪

新增非静态 `ProxyRuntime` 作为进程内唯一资源所有者；`ProxyLifecycleCoordinator`、协议 Server、MessagingProcessor、管理服务和执行器全部由构造器注入并挂在该 runtime 下。禁止把 coordinator 放进 `ConfigurationManager` 或进程级 singleton；PreStop、SIGTERM hook 和两个协议必须引用同一个 runtime 实例和 completion future。

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
- `STOPPING/STOPPED`：SIGTERM hook 只执行一次剩余资源关闭；并发 PreStop、重复 drain 和 TERM 共享同一 future 与 close guard。

初始 readiness 使用一次性 `ReadinessContributor` barrier，至少要求：管理服务与两个业务 listener 已绑定、MessagingProcessor 和 ACL/TLS 已初始化、NameServer 首次同步成功、路由缓存至少成功刷新一次。`proxyWarmupTopics` 非空时，还要为每个关键 topic 完成 route lookup 和 Broker channel 建立；禁止发送真实探测消息。

warmup 超时后 Pod 保持 STARTING/NotReady、记录原因并退避重试，由 rollout timeout 阻止继续替换旧 Pod，不以进程重启制造依赖风暴。首次进入 READY 后，NameServer/Broker 等共享依赖的短暂失败采用 readiness fail-open；只有 listener 关闭、核心 executor 终止、runtime fatal 等 Pod 本地不可恢复故障，或显式 lifecycle 转换，才让 readiness 失败。

### 3.2 管理接口

`enableProxyAdminServer=true` 时新增监听 `0.0.0.0:8082` 的内置管理服务，供 kubelet 和 provider health checker 直接访问 Pod IP；业务 Service 不暴露该端口：

- `GET /started`：本地组件初始化完成后返回 200；startupProbe 使用它，不等待外部 warmup。
- `GET /live`：仅不可恢复故障或 STOPPED 时失败；排空期间保持 200。
- `GET /ready`：只在 READY 返回 200，其余状态返回 503。
- `GET /state`：仅 loopback 返回详细状态、原因、固定 cutoffs、各协议连接数、最后建连时间、send inflight、pending write/open RPC 和 forced 标志。
- `POST /drain?wait=true&waitTimeoutSeconds=N`：仅允许 loopback socket peer；忽略 `Forwarded`/`X-Forwarded-For`。参数只限制 HTTP 调用方等待，不创建或延长 coordinator deadline。

admin 开启但 lifecycle 关闭时，三个 health GET 使用兼容就绪语义，`/state` 明确显示 `lifecycleEnabled=false`，POST drain 返回 409；这只服务首次 bootstrap，不构成严格能力。镜像提供 `mqproxyctl drain --wait --timeout 480s`，通过 loopback 调用管理接口。SIGTERM shutdown hook 加入或启动同一个 `DrainSession`，作为绕过 PreStop 时的幂等兜底。NetworkPolicy 只作第二层保护，远程 POST 必须在应用层拒绝。

### 3.3 连接租约

默认配置：

- 名义连接寿命：300 秒。
- gRPC：`maxConnectionAge=300s`，使用 gRPC 内建 ±10% 抖动，`maxConnectionAgeGrace=30s`。
- Remoting：Channel 建立时分配 270–330 秒租约。
- gRPC 使用标准双 GOAWAY；不发送 RocketMQ Telemetry Reconnect 命令。
- Remoting 5.3.2+ 在租约后的首个 acknowledged request 进入业务处理前返回 GO_AWAY；客户端在新 Channel 上透明重试原请求。
- 空闲过期 Remoting Channel 在无 inflight 时关闭；旧版客户端继续服务到排空 deadline，但记入 legacy 指标。
- 正常扩容不主动驱逐 Pod，只依靠租约让全部活跃连接在约 5.5 分钟内至少重新建连一次。
- drain 的 LB cutoff 后若仍有新 transport 到达，视为 provider 摘流违约且不得进入业务。gRPC transport 通过 Attributes 标记为 late，interceptor 拒绝其全部新 send RPC，最终由 max-age 或 migrationCutoff 的全局 shutdown 终止；公开 `ServerTransportFilter` 没有单 transport close handle，不虚构立即关闭能力。Remoting 5.3.2+ 返回 GO_AWAY，legacy 关闭连接并计入降级指标；晚到连接不得延长本次 drain。

### 3.4 原子准入、双终态与 transport 屏障

新增 `SendDrainGate`，用一个 packed `AtomicLong` 的关闭位和 accepted 计数实现 `tryAcquire()`/`closeAdmission()` CAS 线性化；permit 自身用 CAS once-only 完成。禁止用“先读状态、再加 LongAdder”或轮询两次为零替代原子门闩。

必须始终满足以下不变量：

1. `closeAdmission()` 返回后绝不再产生新 permit；关闭前成功获取的 permit 已计入 accepted。
2. 每个 acknowledged send 独立维护 `backendTerminal` 与 `protocolTerminal`，仅两者都为 true 时才释放 permit。
3. `DRAINED := admissionClosed && acceptedSends==0 && remotingPendingWrites==0 && grpcOpenSendRpcs==0 && protocolIntakeFrozen && transportsTerminated`。
4. cancel、channel/transport close 只能标记 protocol terminal，不能代替尚未完成的 Broker Future；transport 级 callback 永远不能批量释放 send permit。
5. 服务端只证明 Remoting 本地 `ChannelFuture` 终态或 gRPC stream 终态，不声称证明客户端已收到响应；远端收包证明需要应用层 ACK，超出本方案范围。

permit 的 backend 状态使用 `NOT_STARTED -> STARTED -> TERMINAL` 或 `NOT_STARTED -> SKIPPED` 单调 CAS。只有确定 handler 未 dispatch Broker 时才能写 SKIPPED；一旦 STARTED，cancel/close 必须等待真实 Future terminal。

在 QUIESCING/MIGRATING 阶段 gate 保持开放；进入 DRAINING 的线性化动作就是 `closeAdmission()`，随后立即冻结两个协议的新 intake。同步校验失败、未 dispatch、executor reject 等没有 Broker Future 的路径必须显式写入 synthetic backend terminal，不能靠 finally 猜测完成。

**Remoting 实现：**

- `processRequestCommand` 固定顺序为 processor lookup → lifecycle/request 分类 → lease/gate → flow-control reject → 构造/提交 task。在 `ExecutorService.submit` 之前调用可选 `RemotingRequestLifecycleListener.beforeEnqueue`；默认实现为 NOOP，Broker/NameServer 行为不变。
- 返回的非 wire `RequestAdmissionContext` 直接随扩展后的 `RequestTask` 传递；任务执行时通过严格清理的 scoped context 交给 `AbstractRemotingActivity`，由其立即保存到 `ProxyContext` 供异步 callback 使用。禁止使用 channel+opaque side map。
- lease GO_AWAY、`rejectRequest`、submit reject、`RequestTask.stopRun`、`shutdownNow` 回收和 channel-before-dispatch 都必须终止同一个 context；未进 Broker 的路径标记 synthetic backend terminal，并跟踪拒绝响应写回或主动关连接。
- `MessagingProcessor.request` Future 完成标记 backend terminal；统一 tracked writer 在 `writeAndFlush` 前递增 `remotingPendingWrites`，在 `ChannelFuture` 成功/失败、同步抛错或 channel close 时标记 protocol terminal 并递减。`isWritable=false` 只是背压水位，不再静默丢响应；channel active 时仍写并观察 Future，失败时关闭连接唤醒客户端重试。
- DRAINING 后新 send 永不进入 Broker：5.3.2+ 返回 GO_AWAY；legacy 返回可重试错误后关闭并记为 degraded。事件循环 barrier 等待此前 decode/dispatch/write task 入队，再关闭 acceptor、设置现有 channel `autoRead=false`，并设置 drain-frozen attribute，禁止 `channelWritabilityChanged` 再次开启 auto-read，杜绝 drain 判零后出现晚到 `RequestTask`。
- oneway 不进入严格 gate；已开始的 `requestOneway` 只做 best-effort 独立计数，并在方法返回后完成。

**gRPC 实现：**

- gRPC 的 tracer 先于 interceptor 创建，不能让 interceptor 单向“传 token 给 tracer”。`ServerStreamTracer.Factory` 先为 `MessagingService/SendMessage` 创建 per-stream mutable holder，`filterContext()` 把 holder 放入 `io.grpc.Context`；`ServerInterceptor` 从该 Context 取 holder，在 handler 前获取 permit 并 CAS 绑定。业务 Future 与 `streamClosed()` 只操作这个 holder，禁止各自独立计数。
- holder 从 tracer 创建起计入 `grpcOpenSendRpcs`，所以 gate 拒绝和同步失败的 send RPC 也一直统计到 stream terminal；重复绑定、无 holder 或 method 不匹配必须 fail loudly 并进入可观测拒绝路径。
- `GrpcMessagingApplication.sendMessage` 必须先 CAS `NOT_STARTED -> STARTED` 成功才可调用 Broker，`CompletableFuture` 完成后置 TERMINAL；若 ACL/同步校验/cancel 在 dispatch 前结束 RPC，wrapped call/listener 或 `streamClosed` CAS 为 SKIPPED，后到的业务入口因 STARTED CAS 失败而禁止 dispatch。`ServerStreamTracer.streamClosed` 是唯一 canonical protocol terminal；`ServerCall.close` 只记录 response-close intent，cancel/deadline 只记录原因，均不得把已 STARTED 的 backend 提前终止。
- `ServerTransportFilter.transportReady` 只把建连时间和 late 标记写进 transport Attributes，interceptor 从 `ServerCall.getAttributes()` 拒绝 late transport 的 send；不得把 transport filter 当成可关闭 channel 的 API，也不得由 `transportTerminated` 直接释放 holder/permit。
- 进入 DRAINING 后先关 gate，再调用 `Server.shutdown()` 停止接收新 RPC。accepted send 完成后关闭 Telemetry 等非 send 长流，并用剩余绝对预算检查 `awaitTermination`；`false` 必须增加 forced 指标并调用 `shutdownNow()`。
- `ServerCall.close`/`streamClosed` 不是 socket flush 或客户端收包证明。最后一个 send stream terminal 后仍保留有界 transport grace，并以 `Server.awaitTermination` 与客户端侧零观测错误共同验收。

所有协议和执行器只接受同一个 `DrainSession` 的剩余时间；执行器统一 `shutdown -> awaitTermination(remaining) -> shutdownNow`，任何 `false`、中断或丢弃队列都必须可观测。

### 3.5 规范化配置契约

所有配置均为启动时只读；非法组合由 Proxy 启动校验和 Helm schema 同时拒绝。admin 与 lifecycle 分离，以便旧镜像 bootstrap；生命周期总开关关闭时不启用连接租约/准入门，并继续使用旧 probe/PreStop。严格生产 profile 必须同时开启 admin、lifecycle 和全部协议子能力。

| Java key | Helm key | 默认值 | 约束与失败策略 |
|---|---|---:|---|
| `enableProxyAdminServer` | `proxy.lifecycle.admin.enabled` | `false` | 可独立于 lifecycle 开启；首次迁移阶段 A 使用。 |
| `enableProxyGracefulLifecycle` | `proxy.lifecycle.enabled` | `false` | 仅 Cluster mode；严格 profile 必须为 true。 |
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
| `proxyJvmShutdownTimeoutSeconds` | `proxy.lifecycle.jvmShutdownTimeoutSeconds` | `30` | 5–120；超时进入 forced close 并阻断验收。 |
| `proxyLegacyRemotingDrainPolicy` | `proxy.lifecycle.legacyRemotingPolicy` | `SERVE_UNTIL_CUTOFF` | 只允许 `SERVE_UNTIL_CUTOFF`；legacy 永远不进入严格等级。 |

Helm 还必须校验：lifecycle=true 蕴含 admin=true；`remotingAccessAddr` 是稳定 Service/NLB DNS 和端口；PreStop、Pod grace、NLB deregistration、`minReadySeconds`、HPA scale-down period 满足下一节公式；生命周期开启时不能渲染 TCP probe 或 `mqshutdown` PreStop。

### 3.6 单一时钟与默认预算

首次 `beginDrain` 以 `System.nanoTime()` 创建不可变 `DrainSession`，定义 `T0` 和固定 cutoffs；墙钟只用于日志。Pod deletion 通常略早于 T0，因此 PreStop 与 Pod grace 额外保留安全余量。

```text
maxLease          = ceil(300s * (1 + 0.10)) = 330s
lbCutoff          = T0 + 60s
migrationCutoff   = T0 + 60s + 330s + 30s = T0 + 420s
drainHardDeadline = T0 + 420s + 30s = T0 + 450s
preStopWait       = T0 + 480s
podGrace          = deletion start + 540s
rollout/HPA step  >= 600s
```

1. T0：进入 QUIESCING，readiness 失败；同时触发 EndpointSlice/NLB 摘流。
2. 持续观测业务 transport 的最后建连时间，健康检查/管理连接不计入。20 秒 quiet 只用于诊断；无论是否提前 quiet，都持续守到固定 lbCutoff。
3. lbCutoff 后标记并拒绝任何晚到 transport 的业务：Remoting 可关闭 Channel，gRPC 只拒绝 send 并等 max-age/全局 shutdown。cutoff 前最后一条连接的 lease+grace 必须在 migrationCutoff 前结束；任何连接都不能延长 session。
4. 若 lbCutoff 后业务连接已为零，可提前进入 DRAINING；否则在 migrationCutoff 关闭 admission gate、冻结 intake，并等待双终态至 hard deadline。
5. hard deadline 尚未满足 DRAINED 条件则进入 FORCE_DRAINING，强制关闭并让本轮验收失败；PreStop waiter 最迟 480 秒返回非零，不能把强制路径报告为成功。
6. TERM 后使用 30 秒关闭剩余 JVM 资源，并保留约 30 秒 kubelet 余量；`terminationGracePeriodSeconds=540`。

所有 await 都使用 `min(phaseCutoff, hardDeadline) - now`，禁止阶段开始时重新获得完整预算。重复 drain 返回同一 session/future；更短的外部停止预算只能收紧，任何调用都不能延长。单个 send 保持原 request deadline，drain deadline不得延长 Broker 超时。

NLB 450 秒 deregistration/connection-drain 与 Proxy 时钟并行，从 readiness/target 摘除开始计算，绝不能再串行加到 540 秒之后。若 provider 实测最后新连接 p999 超过 60 秒，或健康检查无法直达 Pod 8082，严格 profile 必须阻断上线并整体重算全部预算，不能压缩 send drain。

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
- `proxy_drain_duration_seconds`
- `proxy_drain_deadline_exceeded_total{phase}`
- `proxy_transport_termination_failed_total{protocol}`
- `proxy_warmup_status{contributor}`
- `proxy_forced_close_total{protocol,reason}`

日志按 drain ID 记录状态迁移、固定 cutoffs、初始/最终连接数、legacy 客户端数和 force 原因；禁止使用 client ID 等高基数标签。默认 PrometheusRule 与升级 gate 将 `late connection after cutoff`、deadline exceeded、transport termination failure、forced close 的任一增量视为失败；warmup 超时阻止 rollout 继续。

## 4. Helm 与容器落地

### 4.1 主 Chart：严格生产基线

- 保持 `maxSurge: 1`、`maxUnavailable: 0`，设置 `minReadySeconds: 600`、`terminationGracePeriodSeconds: 540` 和至少 1800 秒的 `progressDeadlineSeconds`。600 秒不是 warmup 延迟，而是必须大于 540 秒终止上界的 rollout 串行化护栏：第二个新 Pod 计入 Available 前，上一个旧 Pod 必须已经退出。
- startup/readiness/liveness 分别使用 8082 `/started`、`/ready`、`/live`；PreStop 执行 `mqproxyctl drain --wait --timeout 480s`，删除 `mqshutdown proxy || true`。
- Helm command、`distribution/bin/mqproxy`、stock/docker `runserver.sh` 全部以 `exec` 传递到 Java；Docker 镜像安装 `curl`。
- 严格模式要求 `replicas` 或 `HPA.minReplicas >= 3`，Service 不得启用 ClientIP affinity 或 `publishNotReadyAddresses`。
- Remoting 必须设置稳定 `remotingAccessAddr`；外部 Remoting 场景为 NLB 增加 8080 listener，禁止发布 Pod IP。
- PDB 改为 values 驱动并推荐 `maxUnavailable: 1`；明确 PDB 只约束 Eviction。
- 修复 Proxy NetworkPolicy selector，8082 只允许节点/kubelet 和 provider health checker；应用层仍依据真实 socket peer 拒绝非 loopback 的 `/state` 与 POST drain。
- 增加 Proxy 跨节点/AZ spread，并验证 `maxSurge=1` 有调度余量。
- 启用 HPA 时不渲染 Deployment `replicas`；`minReplicas>=3`、`stabilizationWindowSeconds>=600`，scale-down policy 只允许每 600 秒 1 Pod。rollout supervisor 在发布期间把 scale-up 与 scale-down 的 `selectPolicy` 都设为 Disabled，等待 HPA `observedGeneration` 和 Deployment desired replicas 稳定后才更新 Pod template，结束后按快照恢复。若发布中必须扩容，先 pause rollout、单独扩容并稳定，再重新冻结 HPA 后 resume。
- 人工缩容由升级脚本逐级减 1，并等待上一 Pod DRAINED/删除完成；直接从 5 缩到 3 不属于严格流程。
- 升级脚本持续断言最多一个旧 Pod 处于 Terminating；若 Kubernetes 版本或控制器行为突破该约束，立即 pause Deployment。本约束必须通过真实集群测试，不能只凭 `maxUnavailable=0` 推断。

### 4.2 NLB 时钟与 health-port 证据门

- AWS 与 ACK NLB 使用 Pod IP 8082 `/ready` HTTP health check，但不创建面向业务的 8082 listener。实现前必须在两个真实 provider 做 spike，证明 target group 可把 health-check port 指向非业务 target port；任一 provider 不支持时，该 profile 在替代拓扑确定前不得宣称严格。
- 默认 target deregistration/connection drain 为 450 秒，不得在 Proxy 响应排空完成前强制断开现有连接。
- provider values 必须锁定 target type、health interval/threshold、cross-zone、externalTrafficPolicy 和连接终止属性。
- 真实环境测量“readiness 失败后最后一个新连接到达”的 p99/p999，并以 p999 校准 60 秒摘流预算。
- NetworkPolicy 开启后必须同时验证 kubelet 与 NLB health checker 可访问 8082，而业务源不能调用管理操作；不得信任源地址转发 header。

### 4.3 Standalone Chart

Standalone 接入相同管理端点、HTTP probes、PreStop、exec 链、显式滚动策略和终止预算，但保留默认单副本并标记为降级环境。它不承诺驱逐、缩容或异常重启对 send 无感。

### 4.4 升级工具

- `upgrade-prod.py` 增加 lint/template/schema、严格副本/PDB/HPA/预算、surge 调度余量、当前 Pod READY、无进行中 drain、previous revision 兼容性和 provider health-port 证据的 preflight。
- 严格模式禁止无监督的 `helm upgrade --atomic`：自动 rollback 会在异常时再触发反向 rollout。脚本以 `helm upgrade --wait` 启动受监控 rollout，持续观察 Pod drain、HPA、NLB 和 SLO；异常时立即 pause Deployment、停止后续删除并保留 READY Pod。
- timeout 至少为 `副本数 × minReadySeconds + terminationGrace + 启动余量`。受控 rollback 是独立子命令，使用相同的串行 drain gate；前一 revision 不具备生命周期时只能执行 bootstrap 级回退，不能声称严格。
- rollout 后检查所有 Pod READY、NLB targets healthy、forced/late-connection 指标无增量、旧 Pod 无 SIGKILL，并继续观察完整 send SLO 窗口后再按快照恢复 HPA scale-up/scale-down。
- 修复生产 PDB values 被模板硬编码覆盖、Proxy placement 空值覆盖全局配置和 NetworkPolicy selector 错误。
- 独立 canary 使用单独 namespace/release、Deployment selector、Service/NLB 和测试客户端入口，不加入主 Service；至少 3 副本，实际删除一个 canary Pod 验证 provider 摘流和三种严格客户端后才能进入 bootstrap。

## 5. 测试与验收

### 5.1 单元与组件测试

所有新增 Java 测试必须使用 `@DisplayName`，先写失败测试再实现：

- 生命周期状态单调性、STARTING 直达停止、重复 drain 复用同一 session、wait timeout 不延长 hard deadline、正常/forced 路径互斥和 once-only close。
- `/started`、`/live`、`/ready`、loopback-only `/state`/drain；伪造 forwarded header 无效；初次 warmup、warmup 超时恢复、READY 后共享依赖波动 fail-open 和本地 fatal fail-close。
- `SendDrainGate` 的 packed CAS：`closeAdmission` 与数千并发 `tryAcquire` 竞态后没有 late permit，backend/protocol 两种完成顺序和重复 callback 均不下溢。
- gRPC builder 的 age/grace；tracer holder 经 `filterContext()` 先注入、interceptor 后 CAS 绑定同一 permit；backend-first、stream-first、cancel-before-backend、deadline、重复 terminal；`streamClosed` 先于 transport termination 时不得发布 DRAINED。
- late gRPC transport 只通过 call Attributes 拒绝 send，并在 migrationCutoff 全局终止；测试禁止假设 `ServerTransportFilter` 能直接 close transport。
- `Server.awaitTermination=false`、中断和长 Telemetry stream 触发可观测 force fallback；正常 lease expiry 与 forced close 指标严格分开。
- Remoting 270–330 秒租约、5.3.2 门槛、GO_AWAY 在业务处理前；已入队未执行的 `RequestTask` 切 DRAINING 后仍被 accepted 计数覆盖。
- Remoting scoped admission context 在线程复用时不泄漏；submit reject、`stopRun`、`shutdownNow` 队列回收、channel-before-dispatch、channel-during-Broker、non-writable、write throw/failure 全部走 synthetic backend/双终态。
- 事件循环 intake barrier 后不能再提交 late `RequestTask`，writability callback 不能重开 auto-read；GO_AWAY/UNAVAILABLE/legacy rejection write 也计入 pending，直到 Future 终态。
- oneway 仅验证 best-effort 独立计数，不混入严格 send gate。
- Helm 渲染断言 legacy、admin-only bootstrap、strict 三套配置及 probe/PreStop、全链路 exec、600 秒 minReady、540 秒 grace、PDB、HPA ownership/scale policy、NLB 属性、8082 不成为业务 listener 和 NetworkPolicy selector。
- 升级工具测试 HPA 双向 freeze/observedGeneration/restore、最多一个 terminating old Pod、pause-on-failure、受控 rollback、两阶段 bootstrap 和 timeout 公式。

### 5.2 客户端矩阵集成测试

5.0.7、5.2.1 和 Remoting 5.3.2 使用独立 JVM/容器启动，避免相同包名版本冲突。测试客户端只使用正式发布依赖，不修改 SDK 代码。统计对象是最终 logical send；底层 attempt/重试错误单独记录，不能把一次失败 attempt 当成最终 send 失败，也不能隐藏耗尽重试后的错误。

核心验收矩阵：

| 维度 | 必测值 |
|---|---|
| Provider | AWS NLB、ACK NLB |
| Client | gRPC 5.0.7、gRPC 5.2.1、Remoting 5.3.2 |
| API | sync send、async send；batch 单独覆盖 |
| Event | 3→5 扩容、单 Pod restart、全量 rollout、5→3 逐 Pod/HPA 缩容 |
| Load | 稳态低负载、目标生产负载、峰值突发；1 KiB、常规大小、5 MiB 消息 |
| Security | 当前生产 ACL/TLS 组合 |

Kubernetes 场景：

- 3 -> 5 扩容。
- 单 Pod restart。
- Deployment 全量滚动更新。
- 5 -> 3 逐 Pod 缩容。
- PreStop 重入、直接 SIGTERM、LB 摘流变慢、drain deadline 超时。
- Remoting 5.2.0/旧客户端存在时的降级路径。
- feature off、新旧 Proxy 混跑、两阶段 bootstrap、受控 rollback、HPA 与 rollout 竞态。
- Broker 慢响应/flow control、Telemetry 长流、warmup NameServer 不可达、NetworkPolicy 开启和 provider 8082 health check。

每个 `provider × strict client × event` 核心 cell 至少执行 10 轮，累计不少于 1000 万次 logical acknowledged send；payload/security/fault 扩展场景每项至少 10 轮并报告样本数。基线使用同版本、同 offered load 的事件前 15 分钟稳定窗口；事件窗从 T0 到最后受影响 Pod STOPPED 后 60 秒。

- send 错误和超时为零观测。
- 事件窗 p99 不超过同负载基线 +100ms，p999 不超过基线 +500ms。
- `forced_close=0`、deadline/late-connection/transport-termination-failure 均无增量，Pod 无 SIGKILL。
- Remoting write Future 与 gRPC send stream terminal 前 permit 不得归零；报告不宣称服务端证明客户端收包。
- rollout 实测同时 Terminating 的旧 Pod 不超过 1，HPA scale-up/scale-down 在 rollout 期间均为 Disabled 且 observedGeneration 已收敛，结束后配置完全恢复。
- 至少 1000 条独立连接时，扩容后 6 分钟内单 Pod QPS CV <= 0.15，任一 Pod 不超过集群均值 1.3 倍。
- 重复消息和 unknown outcome 单独计数，不作为零错误条件的替代；结果报告必须明确 at-least-once 语义，并给出零失败样本的统计置信上界。

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

首次从旧 Proxy 切换存在不可消除的 bootstrap 边界：旧进程没有租约、8082 和 drain 状态，新代码无法反向使其优雅。首次发布采用一次性两阶段受控流程：

1. 在独立 canary release 以生命周期开启状态验证 5.0.7、5.2.1、Remoting 5.3.2、真实 provider 8082 health check 和单 Pod 删除。
2. 低峰提前扩容主集群并确认容量余量；随后冻结 HPA scale-up 与 scale-down，并等待 observedGeneration 收敛。
3. 阶段 A：全量发布具备新能力的二进制，设置 `enableProxyAdminServer=true`、`enableProxyGracefulLifecycle=false`，继续使用旧 TCP probe/PreStop。该轮只承诺尽力无感；完成后逐 Pod 验证兼容态 8082，确保主集群已没有不支持 health port 的旧镜像。
4. 阶段 B：先确认所有现存 target 的 8082 healthy，再在生产 values 显式开启 lifecycle，同时切换 HTTP probe、PreStop、预算和 NLB health check；再完成一次受监控 rollout。该轮仍属于 bootstrap，不计入严格 SLO。
5. 所有 Pod 都运行 lifecycle-enabled 配置后，等待至少一个完整最大租约+grace 周期，并确认 forced/timeout/late-connection 指标为零。
6. 才启用严格 SLO、按快照恢复 HPA scale-up/scale-down 和正常受控 rollout。

出现 send SLO、连接风暴或 provider 摘流异常时，rollout supervisor 立即 pause 后续替换并保留现存 READY Pod。回退必须显式选择同时兼容二进制、probe 和 PreStop 的 Helm revision，并按相同串行 drain 流程执行；回退到无生命周期旧版本重新进入 bootstrap 等级，禁止自动 rollback。

## 7. 实施顺序与阶段出口

每一步独立提交、默认开关关闭；前一步出口未满足不得开始下一步：

1. **证据 spike：** 在隔离分支验证 gRPC stream terminal/`awaitTermination` 边界，以及 AWS/ACK 不暴露业务 8082 listener 的 health-port 能力；产出可重复测试和 provider 实测数据。失败则先改架构，不进入正式实现。
2. **生命周期核心：** 先写 `DrainSession`、`ProxyLifecycleCoordinator`、`SendDrainGate` 和并发测试，再引入 `ProxyRuntime` 所有权、readiness contributors 与管理接口。出口是状态/CAS/deadline 测试全绿，feature off 行为不变。
3. **gRPC 接入：** 配置 max age/grace、tracer holder/filterContext/interceptor permit 绑定、backend/stream 双终态、late Attributes 和全局 transport freeze/await/force；出口是 5.0.7 与 5.2.1 组件测试通过。
4. **Remoting 接入：** 在 `NettyRemotingAbstract` 增加默认 NOOP 的 pre-enqueue listener 和显式 `RequestTask` context，Proxy activity 接入 tracked writer、lease 与 event-loop barrier；出口是 5.3.2 严格路径和 legacy 降级路径通过。
5. **资源与信号：** 统一 runtime shutdown 顺序、执行器有界等待、TERM fallback，并修复 Chart、`mqproxy`、两个 `runserver.sh` 到 Java 的全链路 `exec`。
6. **指标与门禁：** 增加低基数 metrics、PrometheusRule、drain 日志和升级脚本查询；forced/late/deadline 任一增量可以自动 pause rollout。
7. **Helm：** 实现 schema/config、HTTP probes、PreStop、600/540 秒预算、PDB/HPA/placement/NetworkPolicy、AWS/ACK 与 standalone 降级 profile；先做 render/lint/test，不读取或覆盖用户私有环境 values。
8. **升级与矩阵验收：** 实现 canary、两阶段 bootstrap、HPA 双向 freeze、受控 rollout/rollback，最后跑两 provider 的客户端矩阵；只有完整报告满足第 5 节才宣布严格能力可用。
