# RocketMQ Proxy 优雅上下线方案

## 1. 目标与结论

目标是在 Kubernetes Service/LB 后运行 Proxy，覆盖 Java gRPC 5.x 与 Remoting 客户端，使计划内扩容、缩容、滚动重启和单 Pod 删除期间：

- 业务侧不新增 send 异常或超时；
- p99/p999 只有接近基线的波动；
- 已被 Proxy 接纳并发往 Broker 的 send 必须拿到并刷出响应后，进程才能退出；
- 新 Pod 未完成依赖和热点预热前不接流，扩容后长连接能渐进迁移到新 Pod。

推荐方案是 **Proxy 两阶段排空 + Kubernetes 终止编排 + Java SDK 双热连接池**。只增加 shutdown sleep 无法知道请求是否真正完成；只改服务端虽可显著降低错误，但首个命中下线连接的请求仍会承担重连延迟。

生命周期统一为：

```text
STARTING -> WARMING -> READY -> QUIESCING -> DRAINING -> QUIESCED -> STOPPING -> TERMINATED
```

严格承诺只覆盖有响应语义的同步/异步 send；`sendOneway`、SIGKILL、OOM 和节点瞬断不在零失败保证内。

## 2. 现阶段问题清单

当前实现并非完全没有 shutdown：组件框架会逆序关闭，gRPC 会调用 `Server.shutdown()`，Remoting 也已有 GO_AWAY 能力。问题在于这些能力仍是彼此独立的“组件级关闭”，尚未形成从 Pod 摘流、客户端迁移、停止接纳、精确排空到 JVM 退出的闭环。

### 2.1 P0：会直接破坏优雅下线

| 问题 | 当前代码行为 | 对 send 的影响 |
|---|---|---|
| TERM 不一定到达 JVM | `docker-entrypoint.sh` 使用了 `exec`，但后续 `distribution/bin/mqproxy`、`distribution/bin/runserver.sh` 和 `docker/scripts/runserver-customize.sh` 启动下一层时没有继续 `exec`。 | 容器收到 TERM 后，Java shutdown hook 可能不执行；grace period 到期后被 SIGKILL，在途 send 直接断链。 |
| 没有统一生命周期与摘流入口 | `ProxyStartup` 只注册启动和逆序关闭；`StartAndShutdown.preShutdown()` 默认空实现，Proxy 没有 READY/QUIESCING/DRAINING 状态、readiness 或幂等 drain API。 | Pod 仍可能被 Service/LB 选中时，协议组件已经开始关闭；新 send 与资源关闭发生竞态。 |
| 没有可线性化的 send 排空门闩 | 两种协议都没有统一的 admission gate，也没有覆盖“Broker future 结束 + 响应写出终态”的精确在途计数。线程池 active/queue 为空不能证明 send 已经完成。 | 已接纳并可能已写入 Broker 的 send 会在响应返回前被切断，调用方看到超时或未知结果，重试后可能重复。 |
| gRPC 关闭不完整 | `GrpcServer.shutdown()` 忽略 `awaitTermination` 的布尔结果，超时后不 `shutdownNow()`，仍记录成功；`GrpcMessagingApplication.shutdown()` 只 shutdown 五个执行器而不 await；telemetry 又是长期双向流。 | 进程可能在响应未完成时继续关资源，也可能被长流拖到 kubelet 强杀，形成 send 超时和尾延迟尖峰。 |
| Remoting 优雅能力未真正接入 Proxy | `RemotingProtocolServer` 新建 `NettyServerConfig` 时未开启 `enableShutdownGracefully`；即使开启，`NettyRemotingServer.shutdown()` 也只是固定 sleep，而非按在途请求归零。GO_AWAY 还只发给版本高于 V5_3_1 的客户端。 | 默认情况下连接直接进入关闭；新客户端首个命中请求承担 GO_AWAY 重连/重试，旧客户端则可能直接失败，无法满足统一 SLO。 |

### 2.2 P1：会造成扩缩容无效或 send 抖动

| 问题 | 当前行为 | 实际后果 |
|---|---|---|
| 上线没有 warmup readiness gate | 协议端口启动成功后即缺少更细的依赖就绪判断，也没有热点 Topic 路由与 Broker 连接预热。 | 新 Pod 过早接流，第一批 send 承担路由查询、建连、TLS/鉴权等冷启动延迟。 |
| 扩容不迁移已有长连接 | gRPC/Remoting 客户端都倾向长期复用既有连接，Service VIP 不会主动把它们迁到新 Pod。 | 副本数增加不等于 send 流量扩散；旧 Pod 仍然过载，新 Pod 低负载，扩容对存量客户端近似无感。 |
| 下线没有主动客户端迁移 | 服务端没有 drain notice/ACK，客户端没有预热 standby 并原子切换的机制，也没有 `proxyInstanceId` 验证备用连接落到不同 Pod。 | 必须等某次 send 命中坏连接后才重连；即使双连同一 Service，也可能两条连接都落在待下线 Pod。 |
| 对外访问地址缺少稳定性约束 | `remotingAccessAddr` 等对外地址没有强制校验为稳定 Service/LB 地址，配置错误时可能发布本机或 Pod 地址。 | 客户端重连仍指向正在退出的实例，服务端再完善的 drain 也无法完成切换。 |
| 缺少 Kubernetes 生命周期契约 | 仓库没有为 Proxy 提供 readiness/startup probe、PreStop drain、零不可用滚动策略、PDB 和 HPA 缩容节奏。EndpointSlice 更新、PreStop、TERM 也不存在可依赖的全局先后关系。 | 摘流时间不可控，多个 Pod 可能同时退出，或下线窗口内持续收到新连接和 send。 |
| 关闭预算和资源回收不统一 | 各组件使用自己的固定等待，没有共享绝对 deadline；部分执行器只 shutdown 不 await，Remoting 的周期调度执行器也未在当前 shutdown 路径中关闭。 | 多段等待可能叠加超过 `terminationGracePeriodSeconds`；资源提前关或迟迟不退都会放大 send 抖动。 |

### 2.3 P2：问题不可观测，能力边界不清晰

- 目前没有 lifecycle state、精确 send inflight、pending response write、drain notice/ACK、客户端能力分布和 forced shutdown 指标；只能从零散日志猜测是否真的排空，发布后也无法证明“无感”。
- “Broker 已落盘、Proxy 响应丢失”属于未知结果，服务端无法提供 exactly-once；方案必须避免对这类请求声明安全重试，并通过 message key/业务幂等控制重复。
- `sendOneway` 没有响应，无法确认是否排空成功；Local mode 中 Proxy 与 Broker 同进程，重启 Proxy 同时重启 Broker，不能套用独立 Proxy 的严格承诺。

以上问题中，P0 必须先修复才能称为优雅下线；P1 决定扩缩容和重启是否真正对 send 无感；P2 决定该能力能否安全发布和验收。

## 3. Proxy 实现

### 3.1 生命周期与健康检查

- 新增单例 `ProxyLifecycleManager`，原子维护状态、排空 deadline、原因和一次 drain ID；`beginDrain` 必须幂等，重复 SIGTERM 或 hook 不得重复关闭资源。
- 新增独立 `ProxyAdminServer`：`GET /live`、`GET /ready`、`GET /state` 和鉴权的 `POST /drain`。admin port 不加入业务 Service；readiness 只有在两套协议端口已绑定、NameServer/Broker 最小依赖检查通过、配置的热点预热成功且状态为 READY 时返回 200。
- 随镜像提供轻量 `mqproxyctl`，PreStop 通过 loopback 调用 `POST /drain` 并等待 QUIESCED，避免依赖控制面更新与 TERM 的先后顺序。`POST /drain` 使用 Pod Secret 注入的 token，且必须幂等。
- 启动时先启动 MessagingProcessor 和协议 Server，但保持 `WARMING`；对 `proxyWarmupTopics` 查询路由并建立到目标 Broker 的连接，成功后切换 `READY`。空列表仅做 NameServer、路由服务和 Broker 连接的基础预热。
- liveness 只反映进程/事件循环是否存活，不把 Broker 或 NameServer 的短暂不可用当成重启理由；依赖状态只影响 readiness。
- 每个 Proxy 使用 Pod UID 作为 `proxyInstanceId`。gRPC response metadata、Remoting heartbeat response 和 drain notice 都返回该 ID，供客户端验证连接池是否落到不同实例。

### 3.2 无竞态的 send 排空门闩

- 新增 `SendDrainGate`，用一个 packed atomic state/CAS 同时保存 admission-open 位与已接纳数，使“关闭接纳”和“增加在途”线性化；permit 必须幂等释放。另设 `pendingResponseWrites`，覆盖成功响应和 GO_AWAY/UNAVAILABLE 拒绝响应。
- gRPC 在 Server interceptor 中接纳 send。permit 只有在 Broker `CompletableFuture` 终态和 RPC terminal callback 都到达后才释放；客户端取消或 deadline 不能提前忽略仍在执行的 Broker future。响应 flush 由随后的 gRPC `awaitTermination` 保证，不能把 `ServerCall.close` 单独视为刷出完成。
- Remoting 在请求进入执行器前接纳 send；permit 在 Broker 调用完成且响应 `ChannelFuture` 完成后释放。DRAINING 中返回现有 `ResponseCode.GO_AWAY`，同时计入 `pendingResponseWrites`，请求绝不能进入 Broker 转发逻辑。
- 关闭 transport 的条件是 `admittedSends == 0 && pendingResponseWrites == 0`；随后先停 accept/auto-read，再在 event loop 上放置 barrier，确保关闭前没有晚到的 request/write task。
- 指标按 `protocol`、`operation` 记录精确在途数。send 排空与 telemetry、receive 等长流分开；先排空 send，再以可重试状态关闭长流，避免长流永久阻塞进程退出。

### 3.3 两阶段下线时序

1. PreStop 立即执行 `mqproxyctl drain --wait`。Proxy 原子切到 `QUIESCING`、readiness 返回 503、记录初始在途数；不假设 EndpointSlice、外部 LB 和 kubelet 存在严格先后关系。
2. QUIESCING 阶段分别通过 gRPC telemetry drain command 和 Remoting `NOTIFY_PROXY_DRAINING` 主动通知支持客户端。客户端连到不同 `proxyInstanceId`、完成 standby 预热并 ACK 前，旧连接仍可发送且所有请求继续计数。
3. Proxy 至少等待 `proxyEndpointDrainDelaySeconds`，并等待 capable client ACK 或通知上限；该值必须取已验证 LB 环境中“删除/ready=false 后最后一个新连接到达”的 p999 加安全余量，10 秒只是初始默认值。
4. 切到 `DRAINING` 时用 CAS 关闭 admission。两种 interceptor/processor 保持 1 秒 reject grace，把竞态请求明确返回 `request-not-admitted`/GO_AWAY 并刷出；随后 gRPC 才调用 `Server.shutdown()` 发 HTTP/2 GOAWAY，Remoting 停止 accept/auto-read。
5. 等待 `admittedSends == 0 && pendingResponseWrites == 0`，上限 45 秒；再关闭 telemetry、receive 等长流并等待 gRPC transport termination。
6. 状态切到 `QUIESCED` 后 PreStop 返回，kubelet 才发送 TERM；shutdown hook 幂等进入 STOPPING，依次关闭 channel、业务执行器和 MessagingProcessor。每个执行器都执行 `shutdown` + `awaitTermination`，总强制关闭预算 10 秒，Metrics 最后关闭。
7. TERM 若绕过 PreStop，shutdown hook 自己执行同一完整 drain；任一阶段超时才进入强制关闭并增加 `proxy_drain_forced_total`。全部时间从 Pod deletionTimestamp 起计入 120 秒预算。

当前 `grpcShutdownTimeSeconds` 保留兼容；新统一排空配置显式设置时优先，未设置时仍采用旧值作为 transport 等待预算。

### 3.4 Remoting 主动迁移协议

- 新增请求码 `NOTIFY_PROXY_DRAINING` 及请求头：`proxyInstanceId`、`drainId`、`deadlineMillis`、`reason`。它是服务端到客户端的控制请求，客户端 ACK 后 Proxy 记录迁移完成。
- 连接分三类：支持主动 drain notice、仅支持 GO_AWAY（版本高于 V5_3_1）、完全不支持。仅向第一类发送新请求；第二类使用现有 GO_AWAY + transparent retry；第三类在兼容模式下继续接纳至 deadline，严格模式不得存在。
- Proxy 维护活动 channel registry，通知/ACK、超时、旧版本连接数全部暴露指标。严格模式只在所有发送客户端完成升级后启用。

### 3.5 gRPC 主动迁移协议

- 在 `TelemetryCommand` oneof 中以向后兼容方式新增 `ProxyDrainCommand` 与 `ProxyDrainAck`，字段包含 `proxyInstanceId`、drain ID 和 deadline；只向声明 capability 的 Java client 发送。
- capable client 收到命令后先建立并验证不同实例的 READY channel，再 ACK。Proxy 在 ACK/通知窗结束前保持 Server listener 和 admission 打开，因此严格模式不依赖 GOAWAY 后的冷连接重试。
- DRAINING gate 拒绝阶段返回 `UNAVAILABLE` 及 `x-rocketmq-request-not-admitted=true`；`Server.shutdown()` 之后被 transport 直接拒绝、拿不到该 trailer 的请求只走 grpc-java 标准 GOAWAY 语义，不额外声明“安全未接纳”。

### 3.6 容器信号链

- `docker-entrypoint.sh -> mqproxy -> runserver.sh -> java` 必须全链路使用 `exec`，确保 Java 成为 PID 1；或使用经过验收且能向整个进程组转发 TERM 的 init。
- Docker 与 Kubernetes 集成测试必须断言 JVM 收到 TERM、进入同一 drain ID、Pod 未因 grace period 到期收到 SIGKILL。仅验证 shutdown hook 单测不算通过。

## 4. Java SDK 改造

### 4.1 Remoting Java Client

- 把单 endpoint 的单 `ChannelWrapper` 扩展为默认可配置的连接池；兼容默认值为 1，生产严格模式配置为 2。
- 两条连接都完成 TLS/鉴权/心跳后 producer 才视为 warm；send 从非 draining 的 READY channel 中选择在途数更少者。
- 收到 `NOTIFY_PROXY_DRAINING` 后立即把对应 channel 标为 DRAINING，新 send 原子切到 standby，异步创建替代连接并 ACK；旧连接仅在本地在途请求清零后关闭。
- 保留现有 `enableReconnectForGoAway=true` 作为旧 Proxy/旧协议的回退。对 GO_AWAY 的重试始终沿用原请求总 deadline。
- heartbeat response 返回 `proxyInstanceId`；严格模式的 READY pool 必须包含两个不同 ID，重复实例连接要关闭并用带 jitter 的退避重建。达到 warmup deadline 仍无法获得不同实例时，producer 启动失败并给出明确配置错误。

### 4.2 Java gRPC 5.x Client

- 在 `RpcClient` 内部为同一逻辑 Endpoints 维护两个独立 `ManagedChannel`，不改变公开 Producer API；选择 READY 且在途较少的 channel。
- gRPC GOAWAY/连接状态进入 TRANSIENT_FAILURE 时，立即把新 RPC 切到另一条已 READY 的 channel；仅当服务端明确返回 `request-not-admitted` trailer 时做连接级安全重试，避免对“Broker 已接收但响应丢失”的歧义请求额外制造重复。
- 保留当前 producer 级 route/queue 重试策略；连接池切换必须共享原始 request deadline，不额外放大总超时。
- 每条 channel 通过 response metadata/轻量 warmup RPC 获得 `proxyInstanceId`；严格模式同样要求两个不同 ID。收到 telemetry drain command 后先完成 distinct standby，再 ACK。

### 4.3 扩容后的连接再平衡

- 两套 Java 客户端每 60 秒只轮换一条连接，并加入 ±20% jitter；轮换期间另一条持续承载请求，避免同步重连风暴和冷连接延迟。
- Kubernetes Service 必须 `sessionAffinity: None`；严格模式还要求已验证的 LB 能让重建连接落到不同实例。若 VIP 不能满足，应使用 endpoint-aware resolver（Headless Service/xDS/明确实例列表），不能声称双连接等于双 Pod。
- SDK 新增向后兼容配置：`proxyConnectionPoolSize`（默认 1，严格模式 2）、`proxyConnectionRefreshSeconds`（默认 60）、`proxyConnectionRefreshJitter`（默认 0.2）、`enableProxyDrainHandoff`（默认 true）、`requireDistinctProxyInstances`（默认 false，严格模式 true）。
- 扩容均衡使用统计验收：至少 100 条独立 client connection，在 5 分钟观察窗内检查各 READY Pod 的连接数和 QPS skew；不以两个随机轮换周期作为确定性保证。

## 5. Kubernetes 运行约束

- `replicas >= 3`，HPA `minReplicas: 3`；基准压测必须证明任意少一 Pod 后仍有足够 p99 容量余量。
- Deployment 使用 `RollingUpdate`、`maxUnavailable: 0`、`maxSurge: 1`、`minReadySeconds: 15`。
- `terminationGracePeriodSeconds: 120`；PreStop 调用 `mqproxyctl drain --wait --timeout 95s`，其内部覆盖 endpoint/LB 摘流、通知、45 秒 send 排空和 transport quiesce；PreStop 返回后另留 10 秒关闭执行器/Processor，并至少保留 15 秒 kubelet 安全余量。
- readiness/startupProbe 使用 admin HTTP port；startup probe 最长允许 120 秒预热。liveness 使用 `/live`，QUIESCING/DRAINING/QUIESCED 期间继续成功，避免 kubelet 在 drain 中二次杀进程。
- PDB `maxUnavailable: 1` 只约束 Eviction API；滚动发布依赖 `maxUnavailable: 0/maxSurge: 1`，人工运维统一使用 Eviction，不能把 PDB 当成直接删除、HPA 或节点硬故障的保护。
- HPA 使用 `autoscaling/v2`，scale-down 只有 `Pods: 1 / 60s` 一条 SelectPolicy，稳定窗口 300 秒；启用 HPA 后 GitOps 不再管理 Deployment `spec.replicas`。集群必须预留 `HPA maxReplicas + 1` 的 surge 调度容量。
- Service 不得设置 `publishNotReadyAddresses: true` 或 ClientIP session affinity。上线前锁定并记录 LB 产品/版本、Pod 或 Node target type、`externalTrafficPolicy`、健康检查周期与阈值、连接 drain/deregistration delay；方案的严格承诺只适用于该已压测环境。

## 6. 与 `rocketmq-helm` 工程对齐

落地涉及 `/Users/lossend/pro/rocketmq-helm` 中两条部署路径：主 Chart 的 `templates/proxy.yaml` 用于正式 data release，`proxy-sg-standalone/` 及 `upgrade-proxy-standalone.py` 明确面向 testing/SG。严格 SLO 以主 Chart 为生产基线；standalone 未完成同等级改造前只能用于测试，不能声称滚动或缩容对 send 无感。

### 6.1 当前 Chart 能力与缺口

| 范围 | 已有基础 | 必须补齐的问题 |
|---|---|---|
| 主 Chart rollout | 默认 `maxSurge: 1`、`maxUnavailable: 0`，终止窗 120 秒；生产通常 3 副本，India overlay 为 5 副本；ConfigMap checksum 会触发 rollout。 | 缺少 `minReadySeconds`、业务 readiness 和 drain 完成条件；`helm --wait` 当前只等 TCP 端口。 |
| 启动与 PreStop | Pod 有 PreStop，镜像也有 shutdown 脚本。 | 容器通过 `/bin/sh -ec './mqproxy ...'` 启动且没有 `exec`；PreStop 直接执行 `./mqshutdown proxy || true`，只发 kill、不等待排空，并吞掉失败。它与 RocketMQ 内部非 `exec` 启动链叠加后，TERM 不能可靠到达 JVM。 |
| 探针 | 主 Chart 配置 startup/readiness/liveness，standalone 配置 readiness。 | 探针全部是 gRPC 8081 TCP 检查，只能证明 listener 存活，无法表达依赖预热、READY 或 DRAINING；standalone 还缺 startup/liveness。 |
| PDB 与副本 | 生产 values 已配置 `replicas: 3` 和期望的 `pdb.minAvailable: 2`。 | `templates/proxy.yaml` 把 `minAvailable` 硬编码为 1，生产 values 实际不生效；standalone 默认 1 副本且没有 PDB。 |
| 调度与弹性 | 主 Chart 有 hostname 级 required anti-affinity。 | Proxy 被明确排除在 zone spread 之外；空的 `proxy.nodeSelector: {}` 还会阻断 global placement 继承。两个 Chart 都没有 HPA，固定 `spec.replicas` 会与后加 HPA 争夺字段所有权。 |
| Service 与 NLB | ClusterIP 默认不发布 NotReady endpoint、无 ClientIP affinity；生产 gRPC NLB 使用 IP target，ACK 已开启 30 秒 connection drain。 | NLB 仍以 8081 TCP 判活；AWS 未显式配置 target-group deregistration；ACK 的 30 秒没有与 120 秒 Pod 预算和 Proxy drain 对齐。NLB 只暴露 gRPC，Remoting 也未强制发布稳定的 `remotingAccessAddr`。 |
| 可观测与运维 | 主 Chart 已有 Proxy ServiceMonitor；升级脚本使用 `helm upgrade --wait --timeout`。 | 生产 `metricCollectorMode` 关闭，且没有 lifecycle/drain 告警；升级脚本没有容量/客户端能力预检、NLB target 检查、send 冒烟和 rollout 后 SLO gate。standalone 测试只检查 namespace。 |

主 Chart 的 NetworkPolicy 还存在独立问题：模板选择 `component=networkpolicy`，而 Proxy Pod 标签是 `component=proxy`，因此当前策略不会保护 Proxy。修正 selector 时必须同时允许实际业务/NLB 来源和 kubelet health check，并继续禁止外部访问 drain API。

### 6.2 主 Chart 模板改造

- `templates/proxy.yaml` 的启动命令改为 `/bin/sh -ec 'exec ./mqproxy -pc ...'`，同时仍需修改 RocketMQ 镜像内 `mqproxy -> runserver.sh -> java` 的 `exec` 链；只改 Helm 这一层不足以保证 JVM 收到 TERM。
- 用 Downward API 注入 Pod UID 作为 `PROXY_INSTANCE_ID`，Proxy 运行时优先读取它作为 `proxyInstanceId`。新增 admin container port 8082 和 token Secret volume，但不能把 admin port 加入 ClusterIP/NLB 业务 Service。
- PreStop 改为 `/bin/sh -ec 'exec ./mqproxyctl drain --wait --timeout 95s'`，删除 `mqshutdown` 和 `|| true`。命令先使 `/ready` 失败，再通知客户端、等待 Endpoint/LB 摘流、精确 send 排空和 transport quiesce；失败必须留下 Pod event、结构化日志和 forced-drain 指标。
- startup/readiness 改查 admin `/ready`，liveness 查 `/live`；显式设置 `timeoutSeconds`。DRAINING 期间 readiness 失败但 liveness 成功，避免 kubelet 在排空中重复杀进程。
- Deployment 增加 `minReadySeconds: 15` 和可配置 `progressDeadlineSeconds`。`terminationGracePeriodSeconds: 120` 可作为主 Chart 初始值，但必须通过下述 provider 预算校验，不能无条件复用到 standalone。
- PDB 模板读取 `.Values.proxy.pdb.minAvailable`，生产渲染必须得到 2；与 `maxUnavailable` 二选一。增加 `values.schema.json` 或 Helm `fail` 校验，阻止 `minAvailable >= replicas`、严格模式副本少于 3 等无效组合。
- 新增可选 `templates/proxy-hpa.yaml`。HPA 开启时 Deployment 不渲染 `spec.replicas`；默认 `minReplicas: 3`、300 秒 scale-down 稳定窗、每 60 秒最多减少 1 个 Pod，并预留 `maxReplicas + 1` 的 surge 调度容量。
- 把 Proxy zone spread 和 node placement 做成显式 values；修正空 `proxy.nodeSelector` 阻断 global tolerations/nodeSelector 继承的问题。严格模式至少跨 hostname，生产多 AZ 环境还要按容量选择 preferred 或 required zone spread。
- ConfigMap 注入 Proxy graceful lifecycle 配置和稳定 `remotingAccessAddr`。若 Remoting 仅集群内访问，发布 ClusterIP DNS；需要跨集群/公网时必须提供独立稳定 LB，不能发布 Pod IP。
- 修复 NetworkPolicy selector，并把 admin port 约束为 kubelet probe 可达、Pod loopback drain 可用、普通业务客户端不可达。ServiceMonitor 继续抓取 5557 上的生命周期指标，无需暴露 admin port。

### 6.3 建议的 values 契约

以下是新增 Chart values 的目标形态，字段名在实现评审时固定；开关首版保持关闭，先完成 Proxy/SDK 兼容发布：

```yaml
proxy:
  replicas: 3
  minReadySeconds: 15
  progressDeadlineSeconds: 600
  terminationGracePeriodSeconds: 120

  gracefulLifecycle:
    enabled: false
    adminPort: 8082
    adminTokenSecretName: rocketmq-proxy-drain
    preStopTimeoutSeconds: 95
    endpointDrainDelaySeconds: 10   # 上线前替换成实测 p999 + 余量
    sendDrainTimeoutSeconds: 45
    shutdownSafetySeconds: 25

  probes:
    startup:
      path: /ready
      failureThreshold: 30
      periodSeconds: 4
    readiness:
      path: /ready
      periodSeconds: 2
    liveness:
      path: /live
      periodSeconds: 10

  pdb:
    enabled: true
    minAvailable: 2

  autoscaling:
    enabled: false
    minReplicas: 3
    maxReplicas: 20
    scaleDown:
      stabilizationWindowSeconds: 300
      maxPodsPerMinute: 1

  nlb:
    deregistrationDelaySeconds: 60  # 示例值，必须由对应 LB 实测替换
    terminateConnectionsAtDeadline: false  # capable SDK 覆盖率达标后再开启
```

Chart 必须做跨字段校验：严格模式下 `replicas/HPA.minReplicas >= 3`、PDB 至少保留 2 个实例、PreStop 预算大于通知与 send drain 之和，并满足：

```text
terminationGracePeriod
  >= max(Proxy 摘流与排空路径, LB target deregistration 路径)
   + JVM/执行器关闭时间
   + kubelet 安全余量
```

若实测 LB 注销窗口使 120 秒不再满足该公式，必须增加 Pod termination grace，而不是压缩 send drain 或假设连接已经迁移。

### 6.4 NLB provider 对齐

- AWS values 保留 IP target，并显式渲染 `service.beta.kubernetes.io/aws-load-balancer-target-group-attributes`：至少包含 `deregistration_delay.timeout_seconds=<实测值>`；是否启用 `deregistration_delay.connection_termination.enabled=true` 必须与 capable SDK 主动迁移的覆盖率同步。当前只配置 LB attributes 和 TCP health check，target-group drain 仍取环境默认值。
- ACK values 保留现有 connection drain，但把固定 30 秒改为各环境实测值，并与 PreStop/termination 公式统一。connection drain 只能让已建立连接存活一段时间，不能替代 Proxy readiness、客户端迁移和 send inflight 排空。
- 若云控制器支持对 Pod admin port 做独立 HTTP health check，NLB 使用 `/ready`；若不支持，TCP health check 只作为基础存活检查，严格摘流以 Kubernetes readiness/EndpointSlice、主动 drain 和实测 provider 延迟共同判定。
- gRPC NLB 不承载 admin port。Remoting 若需要外部访问，单独建立稳定 Service/LB 并锁定同样的 target type、健康检查和 deregistration 契约。

### 6.5 Standalone Chart 的处理

- `proxy-sg-standalone` 当前默认 1 副本、Kubernetes 隐式 30 秒终止窗、只有 TCP readiness，并且没有 PreStop、显式 rollout、PDB/HPA、anti-affinity 或 topology spread；因此继续标记为 testing-only。
- 若只用于功能测试，可以保留单副本，但测试报告必须明确不覆盖 send 无感 SLO。若要用于生产，则复用主 Chart 的 lifecycle/probe/PDB/HPA/scheduling helper，不能维护第二套弱化实现。
- 增加 schema 校验非空 `namesrvAddr`、`clusterName`、ACL Secret 引用和生命周期预算；当前必填字段为空时仍可能因 8081 已监听而通过 TCP readiness。
- AWS standalone NLB 当前 10 秒检查周期、3 次 unhealthy 阈值且无 target-group drain 属性。它必须与 admin readiness 和显式 deregistration delay 一起改造；否则 Pod 30 秒退出可能早于 NLB 完成摘流。

### 6.6 升级脚本与 Chart 验收

- `upgrade-prod.py` 和 `upgrade-proxy-standalone.py` 增加 preflight：`helm lint/template`、严格模式副本/PDB/HPA/终止预算校验、`maxSurge` 调度余量、当前 Pod 全 READY、无进行中 drain、capable client 比例达到发布门槛。
- 修复 readiness/drain 后再启用 `helm upgrade --atomic --wait --timeout`；timeout 至少覆盖逐 Pod 的预热与 drain 最坏时间。自动 rollback 也会触发一次反向 rollout，必须先完成相同的 drain 集成测试。
- rollout 过程中逐 Pod 记录 Pod UID、drain ID、readiness 变更、EndpointSlice terminating/ready 状态和 NLB target 状态；`maxUnavailable: 0/maxSurge: 1` 保证前一个 Pod 完整退出后才继续，但不能替代这些证据。
- postflight 必须检查所有 Pod 状态为 READY、NLB targets healthy、`proxy_drain_forced_total` 无增量、旧 Pod 无 SIGKILL、send 冒烟成功，并继续观察完整事件窗的失败率和 p99/p999。
- Chart 渲染测试至少断言：启动命令含 `exec`、三种 HTTP probe、PreStop 调用 drain 且无 `|| true`、120 秒预算、`minReadySeconds`、生产 PDB 渲染为 2、HPA 开启时省略 replicas、admin port 不进入 Service、Proxy zone spread、AWS/ACK drain annotations 和 NetworkPolicy selector。
- 集成测试在 kind/测试集群验证 readiness 先于进程退出变为 false、一次只终止一个 Pod、PreStop 超时可观测、直接 TERM 走同一 drain；真实 AWS/ACK NLB 环境再验证“最后一个新连接到达”时间和持续 send SLO。

## 7. 配置、指标与兼容发布

### Server 配置

- `enableProxyGracefulLifecycle=false`：首版默认关闭，完成 SDK 与部署升级后开启。
- `proxyDrainNoticeSeconds=5`
- `proxyEndpointDrainDelaySeconds=10`：仅初始值，上线前替换为具体 LB 摘流 p999 + 安全余量。
- `proxyDrainTimeoutSeconds=45`
- `proxyShutdownForceTimeoutSeconds=10`
- `proxyRejectGraceMillis=1000`
- `proxyWarmupTopics=`：生产填写高频 topic；严格尾延迟验收时不得留空。
- `proxyWarmupTimeoutSeconds=60`
- `proxyStrictDrainClientVersion=false`：能力覆盖率 100% 后开启。
- `proxyInstanceId`：从 Pod UID 注入，必须唯一。
- `proxyAdminPort=8082`、`proxyAdminTokenFile`：admin port 不加入业务 Service，drain token 从 Secret 文件读取。

### 指标与日志

- Server：`proxy_lifecycle_state`、`proxy_inflight_requests{protocol,operation}`、`proxy_pending_response_writes{protocol}`、`proxy_drain_duration_seconds`、`proxy_drain_forced_total`、`proxy_drain_rejected_total{protocol}`、`proxy_drain_notice_total{protocol,capability,result}`、`proxy_warmup_duration_seconds`。
- Client：READY/DRAINING channel 数、handoff 次数、连接建立耗时、按原因重试数、send attempt 与 end-to-end latency。
- 每次 drain 结构化记录 drain ID、状态迁移、deadline、开始/结束在途数、未升级连接数和强制关闭原因；告警条件为 forced drain > 0、DRAINING 超时、READY channel < 2 或发布窗口 send SLO 超标。

### 分阶段启用

1. 先发布 Proxy 生命周期、health、计数和指标，保持 graceful enforcement 关闭。
2. 发布 Remoting Java Client 与 Java gRPC Client 的双热连接、instance identity 和主动 drain capability，生产显式设置池大小 2；观察连接数和资源增量。
3. 上线 Kubernetes probe、滚动策略、PreStop、PDB/HPA 约束，验证 endpoint 摘流时间。
4. 开启兼容模式 drain；旧客户端仍走 GO_AWAY/重连，并通过指标清点。
5. 指标确认 active send connection 100% 支持主动通知、且每个严格客户端拥有两个不同 instance ID 后，开启 strict client version，再执行全量滚动重启。任一步可通过关闭 server 开关回退，客户端双连接可保留。

## 8. 测试与验收

- 单元测试覆盖状态机幂等、packed admission CAS 线性化、pending write、permit 单次释放、deadline、强制关闭和三类客户端 fallback；所有 JUnit 用例用 `@DisplayName` 描述场景。
- gRPC 集成测试用可延迟 Broker 验证：客户端取消后 Broker future 仍计数、RPC terminal 与 backend terminal 都完成才释放、主动通知后 distinct standby READY、reject grace 可返回 safe-not-admitted、长 telemetry 不阻塞 send 排空。
- Remoting 集成测试验证 drain notice/ACK、standby 原子切换、响应 flush 后计数归零、GO_AWAY 回退和老客户端兼容。
- Docker/Kubernetes 集成测试断言 Java 是 PID 1（或 init 正确转发），PreStop 与直接 TERM 都进入 drain，kubelet 未发送 SIGKILL；同时验证 admin drain 鉴权和重复调用幂等。
- 在锁定的真实 LB 环境持续压测 3→6 扩容、6→3 单步缩容、完整 rolling restart、Eviction 单 Pod；每个场景至少重复 10 次，每次至少 100 万次同步/异步 send，并记录 ready=false 后最后一个新连接到达时间。
- 事件窗定义为首次 readiness 变为 false前 60 秒至 Pod 退出后 120 秒；同 QPS、同消息大小的无变更窗口作为基线。报告失败/超时/重复观测值及零事件的 95% Poisson 上界（100 万样本约 3 ppm），不能把“未观测到”表述成数学零风险。
- 发布门槛：观测失败、超时和重复 message ID 均为 0，`proxy_drain_forced_total=0`；事件窗口 p99 不高于 `max(基线×1.10, 基线+5ms)`，p999 不高于 `max(基线×1.20, 基线+20ms)`；至少 100 条独立连接时，扩容 5 分钟后的单 Pod QPS 与理想均值偏差不超过 20%。
- 实施完成后执行针对性 Maven 测试、`mvn clean compile` 和 `ur-format`，再运行真实 K8s/LB 的故障注入验收。

## 9. 明确边界

- 该设计保证计划内、收到正常终止信号的上线/下线；基础设施硬故障只能依赖客户端现有重试和 Broker 语义。
- `sendOneway` 无法判断 Broker 是否已接收，不承诺零丢失/零重复；需要严格保证的调用方应迁移到有确认的异步 send。
- 严格尾延迟目标依赖主动通知、两个不同 `proxyInstanceId` 的 READY 连接、至少三个副本、单 Pod 故障容量余量、热点预热和已验证 LB；缺任一条件只能降级为“力求零额外失败、允许一次重连抖动”。
- GO_AWAY-only 和完全 legacy Remoting 客户端不属于严格 SLO 人群；兼容模式保可用，严格模式必须通过连接能力指标阻止开启。

## 参考依据

- Apache RocketMQ GO_AWAY 设计与客户端透明重连：[Issue #7330](https://github.com/apache/rocketmq/issues/7330)、[PR #7467](https://github.com/apache/rocketmq/pull/7467)
- Kubernetes Pod 与 Endpoint 终止流程：[官方教程](https://kubernetes.io/docs/tutorials/services/pods-and-endpoint-termination-flow/)
- Kubernetes PreStop 与 grace-period 语义：[Container Lifecycle Hooks](https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks)
- Kubernetes gRPC probe：[Liveness, Readiness, and Startup Probes](https://kubernetes.io/docs/concepts/workloads/pods/probes/)
- AWS Load Balancer Controller target-group attributes：[Service annotations](https://kubernetes-sigs.github.io/aws-load-balancer-controller/latest/guide/service/annotations/)
- Alibaba Cloud NLB connection draining：[NLB health checks](https://www.alibabacloud.com/help/en/slb/network-load-balancer/user-guide/nlb-health-check-overview/)
