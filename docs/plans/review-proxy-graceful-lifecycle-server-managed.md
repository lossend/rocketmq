# Proxy Graceful Lifecycle Server-Managed 方案复审

> 评审对象：[`plan-proxy-graceful-lifecycle-server-managed.md`](./plan-proxy-graceful-lifecycle-server-managed.md)
> 复审日期：2026-07-27
> 证据基线：当前 RocketMQ 源码、Java Client 5.0.7/5.2.1、当前 Helm 生产值，以及 Kubernetes、AWS NLB、ACK NLB 官方行为
> 结论状态：**有条件可行；4 个 P0 关闭前不能作为实施基线**

## 0. 结论摘要

方案的大方向合理：

- 生命周期由 Proxy 统一管理；
- 用显式状态机、双终态和 deadline 控制停止过程；
- 区分停止接流量、迁移连接、排空已接收请求、释放资源；
- 用真实客户端和真实负载均衡器验证零主动失败目标。

但当前计划仍把以下三类能力混成了一个“GOAWAY 屏障”：

1. gRPC `Server.shutdown()`：全局、一次性、释放 listener，不能在 READY 状态可逆调用；
2. Remoting `GO_AWAY`：仅对版本高于 `V5_3_1` 的非 oneway 请求返回，由客户端在 transport 层重连并重放一次；
3. 应用层 `SendDrainGate`：只管理已经进入业务分发路径的请求，不等价于连接迁移完成。

因此不能先写死“某一时刻同时 GOAWAY 并关闭共享 gate”，也不能把 gRPC `Server.shutdown()` 当成可重复的自我驱逐 API。正确做法是先用 spike 证明每个协议的 no-new-work 边界，再固定唯一的关闭顺序。

本次复审结果：

| 级别 | 数量 | 含义 |
|---|---:|---|
| P0 | 4 | 不关闭就无法证明零主动失败或无法安全实施 |
| P1 | 8 | 必须在主计划中补齐，否则存在明显运行或交付风险 |
| 优化项 | 6 | 不阻塞首轮实现，但应进入验收标准或后续任务 |

## 1. P0：实施前必须关闭

### P0-1：协议迁移屏障和 admission 顺序尚未闭合

#### 已确认事实

**gRPC**

- grpc-java 公共 API 只有全局 `Server.shutdown()`，它会释放监听 socket、拒绝新 call，并开始优雅终止；
- 同一个 `Server` 不能在 shutdown 后重新 start；
- `ServerTransportFilter` 只有 transport ready/terminated 回调，没有单 transport 的 close/GOAWAY handle；
- 在“不手写 HTTP/2 帧、不依赖 grpc-java 私有 API”的边界下，无法实现按连接分批 GOAWAY，也无法实现 READY 状态下可恢复的 GOAWAY-only。

**Remoting**

- 服务端 `GO_AWAY` 是请求到达后的响应，不是可主动广播并等待 ACK 的协议屏障；
- 当前 shutdown 分支只对 `cmd.version > V5_3_1` 生效，旧客户端仍会继续 dispatch；
- `writeResponse()` 对 oneway 请求直接返回，因此 shutdown 分支会跳过业务 dispatch、又不会真正写出 `GO_AWAY`；客户端也没有响应可据此 replay，形成静默丢失；
- `NettyRemotingClient.invokeImpl` 已明确实现一次 transport 层重连和请求重放，目标仍是同一个逻辑地址；
- 第二次收到 `GO_AWAY` 会失败，因此“重放一次”只能缓冲单次迁移，不能承载正确性；
- 当前 `NettyRemotingServer` 没有持有完整的 child `ChannelGroup`，acceptor channel 也不是可供 drain 协调器使用的长期字段。

**共享 gate**

- gRPC 和 Remoting 共享业务处理链，但连接迁移语义不同；
- gRPC `awaitTermination()` 只能证明 gRPC Server 终止，不能证明 Remoting 已没有新请求；
- Remoting 在返回 `GO_AWAY` 前仍需读取请求，不能把“关闭应用 gate”当成它的迁移 ACK。

#### 必须修改

主计划应只保留下面这一套规范，不再同时出现“GOAWAY 与 gate 同时关闭”和“GOAWAY 完成后再关 gate”两种顺序：

1. 进入 `QUIESCING`，冻结本次 drain 的配置快照和 deadline；
2. 触发 provider/Kubernetes 停止新连接，但保证既有连接仍可使用；
3. 分协议启动连接迁移；
4. 每个协议到达经过验证的 no-new-work 边界；
5. 关闭或冻结对应协议的业务 admission；
6. 在 `DrainDeadline` 内等待已经获得 admission 的请求归零、停止业务 listener，并到达 `DRAINED` 或 `FORCED`；
7. 收到 TERM 后再启动 `StopRun`，在独立的 `StopDeadline` 内关闭 admin、metrics、剩余 worker 和进程资源。

这里的第 4 步目前是待验证项，不能用想象出的“GOAWAY ACK”代替。若 spike 证明公共 API 下无法同时满足：

- 不接收新的业务工作；
- 不主动拒绝仍可能落到本 Pod 的合法请求；
- strict client 不出现 `UNAVAILABLE`；

则必须在以下方案中明确选择：

- 放宽零失败目标并定义错误预算；
- 修改客户端重试/`waitForReady` 契约；
- 引入新的 transport 能力或拆分 listener；
- 调整 provider 摘流方式，使新请求在 listener 关闭前已可证明不会到达。

Remoting 还必须按 `clientVersion × sync/async/oneway` 单独选择策略。当前协议下，oneway 不能依赖 `GO_AWAY` 达成零丢失；首期必须修改协议/客户端、排除或禁用 oneway，或者证明 provider no-new-arrival 后再关闭 admission。

#### 验收证据

- gRPC：真实 Java Client 5.0.7/5.2.1，在 shutdown、并发新 call、listener 拒连、in-flight call 下验证；
- Remoting：同一 NLB 地址重连、第二次 `GO_AWAY`、并发、batch、5 MiB、剩余 timeout 场景；
- 服务端：按协议记录 `migration_started`、`no_new_work_reached`、`admission_closed`、`accepted_inflight_zero`；
- 不允许把 `awaitServerTermination()` 或“已发送 GOAWAY”单独当作跨协议屏障。

### P0-2：Kubernetes、应用和 provider 的连接保留时钟没有统一

当前短预算设想中，应用可能在 T0+60 秒才开始迁移连接；但当前 ACK 生产值的 connection-drain timeout 是 30 秒。若 provider 从 endpoint 摘除开始计时，它可能在应用 GOAWAY 前主动断开既有连接。

Kubernetes 的 Pod grace period 在执行 PreStop 前已经开始倒计时，EndpointSlice 的 terminating/ready 变化与 PreStop 也可能并行传播。因此不能把这些阶段简单串行相加后假定顺序。

主计划需要定义并由同一配置源渲染：

- `D_app_hard`：应用 drain hard deadline；
- `D_provider_keep`：provider 保留既有连接的最短时间；
- `D_prestop`：PreStop 最长等待；
- `D_process_stop`：TERM 后资源停止预算；
- `D_dependency_wait`：dependency unhealthy 后允许原地恢复的最长等待；
- `D_skew`：EndpointSlice、controller、health check 和观测传播裕量；
- `D_safety`：生产抖动裕量。

至少满足：

```text
D_provider_keep >= D_app_hard + D_skew + D_safety

# dependency unhealthy 已启动 provider drain 时
D_dependency_wait + D_app_hard + D_skew + D_safety
  <= D_provider_keep

terminationGracePeriodSeconds
  >= D_prestop + D_process_stop + D_safety
```

`D_prestop` 若等待应用 drain 完成，必须小于 Pod grace；应用、PreStop 和 rollout supervisor 必须引用同一份派生值，禁止分别复制默认常量。

#### Provider 必须分开建模

| Provider/路径 | 需要锁定或验证的行为 | 当前结论 |
|---|---|---|
| AWS NLB：target deregistration | `deregistration_delay.connection_termination.enabled=false`，验证 deregistration delay 结束后既有连接行为 | 需显式配置并抓包验证 |
| AWS NLB：target unhealthy | `target_health_state.unhealthy.connection_termination.enabled=false`，避免 unhealthy 时主动终止既有连接 | 需显式配置 |
| AWS NLB：all targets unhealthy | 会 fail-open 到所有 registered targets | 不能替代应用层 fleet 协调 |
| ACK NLB：connection drain | timeout 到期后会主动关闭既有连接 | 当前 30 秒短于计划迁移时钟，不安全 |

AWS 与 ACK 不能共用一句“关闭 unhealthy termination 后连接一定安全”的结论。每个生产 provider 都要有独立的配置、实测 P95 和 packet-level 证据。

官方依据：

- [Kubernetes Pod and Endpoint termination flow](https://kubernetes.io/docs/tutorials/services/pods-and-endpoint-termination-flow/)
- [AWS NLB target group attributes](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/edit-target-group-attributes.html)
- [AWS NLB target group defaults](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-target-groups.html)
- [AWS NLB fail-open behavior](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-troubleshooting.html)
- [ACK NLB connection draining](https://www.alibabacloud.com/help/en/slb/network-load-balancer/user-guide/create-and-manage-a-server-group)

### P0-3：事务消息的 Proxy 亲和性没有可执行解法

风险成立，但旧建议“只豁免事务 Producer 的 lease”不可实施：

- `ClusterTransactionService` 只在本 Proxy 的 `ProducerManager` 仍有本地 producer group 时向 Broker 维持心跳；
- Broker 的 transaction check 回到注册的 Proxy 后，`ProxyClientRemotingProcessor` 仍依赖该 Proxy 本地 producer channel；
- grpc-java `maxConnectionAge` 是 Server 全局配置，不知道 transport 后续会承载哪些 producer group；
- 同一 transport 可以复用多种请求，当前又没有事务专用 listener 或 SDK 变更。

主计划必须在编码前选择并记录一种可实施策略：

1. 全局关闭或显著拉长 steady-state gRPC lease，并重新定义扩容重平衡 SLO；
2. 引入事务专用 listener/路由或其他明确的亲和性边界；
3. 首期明确排除事务消息，并用生产流量 inventory 作为发布门禁；
4. 修改客户端/注册协议，使 transaction check 不再依赖原 Proxy 的本地 channel。

在当前边界下，不得继续把“按事务 Producer 豁免连接 lease”写成推荐实现。

### P0-4：strict client 的零失败结论仍缺真实证据

Java Client 5.0.7 和 5.2.1 都调用了 `disableRetry()`。没有 `waitForReady` 时，RPC 在 `TRANSIENT_FAILURE` 可立即失败；应用层 `maxAttempts=3` 又是无 backoff 的快速重试，可能仍命中同一个 channel/Endpoints。

因此：

- listener 关闭后的 TCP connect failure 不能直接宣称“只会触发 subchannel 重连，不会产生 RPC 失败”；
- gRPC GOAWAY 不等于所有 in-flight、新建和尚未分配 transport 的 RPC 都透明成功；
- `maxAttempts=3` 只能作为偶然缓冲，不能作为生命周期正确性屏障；
- `maxAttempts=1` 会更早暴露竞态，但不是唯一需要满足的客户端配置。

必须建立真实客户端矩阵：

| 协议 | 场景 | 客户端配置 | 硬门禁 |
|---|---|---|---|
| gRPC | shutdown 前后并发 send、listener 拒连、in-flight、连接重建 | 5.0.7/5.2.1，`maxAttempts=1/3` | 零 observed 主动 send failure |
| gRPC | 有/无空闲连接、单连接/多连接、不同并发度 | `waitForReady=false` 的真实默认路径 | 零 observed 主动 send failure |
| Remoting | 首次和第二次 `GO_AWAY`、同 NLB 目标回流 | transport replay 开/关、客户端版本边界 | 行为与预期一致，无重复发送 |
| Remoting | sync/async/oneway、batch、5 MiB、timeout 临界点 | 生产等价配置 | oneway 必须先有协议/范围决策；其余无协议外重放和超时放大 |

如果 strict 矩阵出现失败，应回到 P0-1 改协议、SDK、provider 或目标，不得用“客户端一般会重连”关闭问题。

## 2. P1：主计划必须补齐

### P1-1：steady-state lease 是容量/SLO 取舍，不是无条件 P0

五分钟 `maxConnectionAge` 会带来 TLS、HTTP/2、CPU 和连接重建成本，但它也承担六分钟扩容重平衡目标。应通过压测决定关闭、延长或保留，不能仅凭静态判断改成 1800 秒。

验收至少包括：

- Pod 连接数 CV，而不是请求 QPS CV；
- TLS handshake、CPU、GC、连接建立失败和 reconnect 峰值；
- 扩容后达到目标连接分布的 P95/P99 时间；
- 事务消息策略与 lease 配置的联合影响。

### P1-2：ReceiptHandle 已有主动清理，缺口是 bounded await 和可观测性

当前 `DefaultReceiptHandleManager.shutdown()` 已调用 `clearAllHandle()`，并通过 `CLEAR_GROUP` 主动执行 change-invisible-time；“只等自然过期”这一旧判断不成立。

主计划应补：

- 收集并等待现有 clear task/Future，受统一 `StopDeadline` 约束；
- 超时后记录 remaining handle/group 数和 forced 原因；
- 验证 schedule/renew/return worker 的停止顺序；
- 用 redelivery/重复消费指标评估影响。

消费语义是否进入本项目的严格零失败门禁属于范围决策，不应在没有产品范围说明时直接升为 P0。

### P1-3：双 readiness 方向可保留，但不能绑定可逆 GOAWAY 自我驱逐

建议端点语义：

| 端点 | 谓词 | 使用方 |
|---|---|---|
| `/live` | 进程和 admin loop 可响应 | kubelet liveness |
| `/ready` | lifecycle 允许成员资格、业务 listener 已绑定、无 fatal | kubelet readiness/EndpointSlice |
| `/ready-for-traffic` | startup/warmup barrier 已完成、业务 listener 已绑定、依赖健康、无 fatal；仅允许 READY，以及经验证可保留连接的计划内 QUIESCING/DRAINING | provider health check |

当前 Helm 的 startup/readiness/liveness 都是业务 gRPC 端口的 `tcpSocket`；AWS 生产值也是 TCP 8081 health check，ACK 未显式配置 HTTP health check，默认同样不会调用上述端点。实施任务必须：

- 暴露命名的 admin health port；
- 将 kubelet probes 改为对应 HTTP path；
- 分别为 AWS、ACK 配置 health-check protocol、port 和 path；
- 在模板测试中断言生成值，避免只实现 Java endpoint 却仍由 TCP 探针控制流量。

配置依据：[AWS Load Balancer Controller Service annotations](https://kubernetes-sigs.github.io/aws-load-balancer-controller/latest/guide/service/annotations/)、[ACK NLB annotations](https://www.alibabacloud.com/help/en/slb/network-load-balancer/use-cases/configure-nlb-instances-by-using-annotations)。

`/ready-for-traffic` 在计划内 drain 时是否继续返回 200，只能在以下条件都成立时采用：

- EndpointSlice/provider deregistration 已停止新连接；
- provider 被证明不会因此终止既有连接；
- STARTING 和 STOPPED 阶段绝不会提前/继续报 200。

依赖故障时可以先通过 provider health 停止新连接，但不得在 READY 状态复用 `Server.shutdown()` 做“可重复 GOAWAY 后恢复”。可选策略只有：

- 在 provider 已证明的连接保留窗口内等待依赖恢复；
- 对可证明的 Pod-local 持续故障进入不可逆 drain，并由 Pod replacement 恢复；
- 另立完整的 listener rebuild 状态机和客户端迁移设计。

ACK 开启 connection drain 且 unhealthy 已启动倒计时时，恢复等待必须满足 `D_dependency_wait <= D_provider_keep - D_app_hard - D_skew - D_safety`，为后续不可逆 drain/客户端迁移预留完整预算；当前 30 秒不能支持任意时长恢复等待。超时后必须执行预先定义的不可逆替换/客户端迁移策略，或者改 provider 配置并重新实测。AWS 则需要锁定 unhealthy termination 属性并验证实际连接保留行为。

共享依赖故障时，所有 Pod 同时 503 后再同步 GOAWAY 会形成 fleet reconnect storm。NLB all-target-unhealthy fail-open 仍会把连接路由回同一批 unhealthy targets，甚至不能保证停止新连接，所以它不能替代 quorum/fleet-aware 协调。

### P1-4：admin 两线程模型会被 blocking waiter 饿死

原计划允许 `POST /drain?wait=true` 最长阻塞数分钟，而 admin server 只有固定两个线程。两个并发或重入 waiter 就可能让 `/live`、`/ready` 无线程可用，进而被 kubelet 在 drain 中杀死。

首选契约应收敛为：

- `POST /drain` 只创建或复用一次 `DrainRun`，立即返回 `202 + runId`；
- `GET /drain/{runId}` 轮询结果，所有 handler 都必须短时、非阻塞；
- 设置有界队列、请求并发上限和超时，并在 POST flood 下验证 `/live` 最大延迟。

JDK `HttpServer` 只有 server 级 `setExecutor`，不能直接给不同 `HttpContext` 分配 executor。若仍要保留 blocking waiter，health 必须使用另一个 `HttpServer`/listener 或显式 dispatcher 和独立执行资源，不能只写“保留一个线程”。admin executor 自身也必须服从 `StopDeadline`。

### P1-5：进程静态 executor 和 worker 必须有 deadline-aware stop

`ThreadPoolMonitor.shutdown()` 当前只有 `shutdown()`，没有 bounded await 和 force stop；原计划的资源清单也没有明确修改这个类。这会让 `STOPPED` 证据早于真实资源收尾，丢失最后的清理/观测结果；若后续在 hook 中增加无界 await，卡住的任务还会直接耗尽 Pod grace。

主计划需要：

- 枚举 process-static executor、listener、client、worker 和 scheduler 的唯一 owner；
- 每个资源执行 `shutdown -> await(remaining) -> shutdownNow/close`；
- 所有 await 只消费同一个绝对 `StopDeadline` 的剩余时间；
- 记录首次失败，继续 best-effort 清理其余资源；
- 测试重复 stop、部分 start 失败、卡死 task 和 forced terminal。

### P1-6：最终 metrics 不能作为 rollout 控制的唯一来源

`ProxyMetricsManager.shutdown()` 当前调用异步 `forceFlush()`/`shutdown()` 后没有等待结果；Prometheus 也不存在“关闭 metrics 最后就一定被最终 scrape”的屏障。

改进要求：

- OTLP/LOG exporter 在 remaining deadline 内等待 `CompletableResultCode`；
- PROM 仍暴露 phase/forced 指标，但 rollout supervisor 不能只依赖最后一次 scrape；
- loopback admin state 在 Pod 退出后不可达、内存结果也会消失，不能直接写成 supervisor 的主判据；
- 必须选定一个可达且持久的结果交接：结构化 termination message 并由串行 supervisor 确认、Pod Condition/CRD，或删除前通过受认证的 Pod-IP endpoint/exec 读取；
- rollout pause 以该已确认的 drain result 为主，指标为辅，并定义交接写入/读取失败时 fail-closed；
- 明确 missing series、stale series、query timeout 和 controller restart 时的 fail-closed 行为。

### P1-7：Remoting 需要明确实现缝和 hot-path 预算

不能继续把 Remoting replay 层写成未知。已知事实是：

- 版本高于 `V5_3_1` 的非 oneway 客户端在 transport 层重连同一个逻辑地址并重放一次；
- oneway 在当前 shutdown 分支会被静默跳过，旧版本则继续 dispatch；
- 第二次 `GO_AWAY` 失败；
- 服务端当前没有可供 drain 协调器使用的 retained acceptor channel 和 child `ChannelGroup`。

主计划应明确新增：

- retained server channel；
- child channel tracking；
- event-loop 上按 `clientVersion × invocation mode` 分支的 draining 标记和可观测 barrier；
- 对可安全响应的请求返回 `GO_AWAY` 而不进入业务 dispatch，并为 oneway/旧版本提供明确替代路径；
- freeze/close 的 deadline 行为。

同时基准测试每次 send 的 gate/acquire 原子操作、ChannelGroup 遍历、并发关闭和 5 MiB 请求，不得在没有数据时断言上游 hot path 一定不能接受。

### P1-8：缩小交付范围，但保留真实依赖关系

可以采用以下简化：

- 若生产明确使用固定 replicas，则删除 HPA freeze/unfreeze 状态机；
- rollout supervisor 启动前检查目标 workload 没有 live HPA，而不是给 Proxy 增加 HPA schema；
- 只做一次 annotation 驱动的 bootstrap rollout，不要先改 template 再单独重启制造两次有损滚动；
- `ProxyRuntime` ownership/state machine 可以拆成前置 PR，但它是协议 drain 的真实依赖，不能写成“无因果关系”；
- `forced-with-no-pending-send` 的发布策略必须明确为阻断、人工确认或告警继续，不能依靠评审稿替产品做决定。

仓库测试规则保持不变：

- 新测试继续使用 `@DisplayName`；
- Surefire 2.19.1 的正确属性是 `-Dsurefire.failIfNoSpecifiedTests=false`；
- 只有 `test-compile` 证明缺少显式依赖时，才调整 JUnit/Mockito test dependency。

## 3. 优化项

### R1：重平衡指标改为连接数 CV

请求 QPS 受业务流量影响，不能证明长连接是否重新分布。使用每 Pod 活跃连接数、transport 创建/关闭速率和连接数 CV。

### R2：所有预算由一个 helper/公式派生

`proxyLbDetachTimeoutSeconds` 已经是配置项，缺口不是“去掉硬编码变量”，而是 Helm、应用、PreStop 和 supervisor 不应各自复制默认值。启动时交叉校验：

- provider keep timeout；
- Broker heartbeat/registration timeout；
- send drain deadline；
- PreStop 和 Pod grace；
- exporter/worker stop budget。

任何不满足安全不等式的配置应 fail-fast。

### R3：补齐触发源、阶段耗时和 cutoff 快照

至少区分：

- `PRESTOP`、`SIGTERM_FALLBACK`、`ADMIN`、`DEPENDENCY_FAILURE`；
- 每一阶段的开始、完成、超时；
- effective deadline 和剩余时间快照；
- accepted/rejected/migrated/forced 数量；
- 首个失败资源和最终 terminal。

### R4：不要设计不存在的 gRPC 分批 GOAWAY

gRPC reconnect storm 通过 Pod 串行 drain、容量 headroom、客户端真实 backoff 行为和压测控制。除非明确放宽 API 边界，否则不写“按连接分片发送 GOAWAY”。

Remoting 可以按 child channel 批处理，但仍要验证遍历成本、客户端回流到同一 Pod 的概率和第二次 `GO_AWAY`。

### R5：容量门禁使用不等式，不使用魔法副本数

不要简单规定“最少 4 replicas”。应保证在一个 Pod draining 加所选 failure-domain 损失后：

```text
remaining_ready_capacity
  >= peak_required_capacity * safety_factor
```

同时约束 `maxUnavailable=0`、surge 是否落在独立 failure domain、Pod headroom 和连接重建峰值。

### R6：零 observed failure 保持硬门禁，统计上界只作补充

分层执行 deterministic unit、protocol integration、真实客户端 E2E、真实 NLB rollout 和长时间 soak。零 observed 主动发送失败仍是当前目标；可以额外报告在给定样本量和置信度下的失败率上界，但不能用 `<= 1e-6` 偷换原目标。

## 4. 建议实施顺序

### Step 1：先完成三个证据 spike

1. gRPC shutdown/connect/in-flight 的真实客户端矩阵；
2. Remoting 同 NLB 地址回流、第二次 `GO_AWAY` 和大请求矩阵；
3. AWS、ACK、Kubernetes endpoint/PreStop 并发传播和连接终止抓包。

Spike 失败时先修改架构，不进入批量编码。

### Step 2：冻结不变量和单一预算模型

- 定义协议级 no-new-work 条件；
- 定义 admission、accepted inflight 和双终态；
- 定义 provider/app/PreStop/process 的派生公式；
- 删除文档中的第二套关闭顺序。

### Step 3：关闭事务和消费范围决策

- 选择事务亲和性策略或明确首期排除；
- 确认 ReceiptHandle/redelivery 是否进入严格发布门禁；
- 用生产 inventory 验证范围假设。

### Step 4：实现 runtime、admin、worker 和 metrics ownership

- 先落 `ProxyRuntime`、`DrainRun`、`StopRun`、`StopDeadline`；
- 修复 admin waiter 饥饿；
- 为所有 executor/worker 增加 bounded stop；
- 为 rollout supervisor 提供可达、持久且可确认的 drain result 交接。

### Step 5：按协议实现迁移

- gRPC 只使用经 spike 验证的公共 API 顺序；
- Remoting 增加 retained channel、child tracking 和 event-loop barrier；
- 不用单个共享 gate 冒充跨协议 GOAWAY 屏障。

### Step 6：一次性完成 Helm/bootstrap

- 固定 replicas 环境删除 HPA 编排，但保留 preflight；
- 从同一 helper 派生 provider timeout、PreStop 和 grace；
- 增加 admin health port，并将 kubelet、AWS、ACK 探针显式切到正确的 HTTP port/path；
- 一次 annotation bootstrap rollout 后再启用严格门禁；
- rollout 串行并验证容量不等式。

### Step 7：分层验证并锁定发布门禁

- unit/integration 使用仓库既有测试规范；
- 真实客户端覆盖 `maxAttempts=1/3`；
- 真实 provider rollout 观察 packet、RPC、Remoting 和 metrics；
- forced、deadline、missing metrics 或 strict send failure 均按预先定义的 fail-closed 策略暂停。

## 5. 应保留的原方案设计

- `SendDrainGate` 的 CAS 线性化思想，但作用域必须限定为应用 admission；
- `DrainRun`/`StopRun` 单次执行与并发调用复用；
- `DRAINED`/`FORCED` 双终态和绝对 deadline；
- 资源 single ownership、逆序关闭和 best-effort cleanup；
- 不依赖 grpc-java package-private API、不手写 HTTP/2 帧；
- annotation 驱动的兼容性 bootstrap；
- 先测试后实现以及真实客户端/真实 LB 验收。

## 6. 本次纠正的旧结论

以下内容已从本评审删除或降级，不应再进入实施任务：

- “ReceiptHandle 关闭时只等待自然过期”——错误；已有 `clearAllHandle/CLEAR_GROUP`；
- “按事务 Producer 豁免 gRPC lease”——当前 Server 全局配置下不可实施；
- “READY 状态可重复调用 gRPC GOAWAY 并恢复”——`Server.shutdown()` 不可逆；
- “按连接分批发送 gRPC GOAWAY”——公共 API 不支持；
- “listener 拒连对 strict client 一定无 RPC 失败”——未被真实客户端证明；
- “Remoting replay 所在层未知”——代码已确认是一次 transport 层 replay；
- “所有 Remoting 请求都能收到 `GO_AWAY` 并 replay”——旧版本和 oneway 不满足；
- “移除 `@DisplayName`”——违反仓库测试规范；
- “Surefire 2.19.1 不支持 prefixed property”——错误；
- “固定副本数环境仍必须实现 HPA freeze/unfreeze”——不必要，但要做 live HPA preflight；
- “用统计错误率替代零 observed failure”——改变了原始发布目标。

## 7. 问题索引

| ID | 级别 | 结论 |
|---|---|---|
| P0-1 | P0 | 协议迁移屏障和 admission 顺序未闭合 |
| P0-2 | P0 | provider/app/Kubernetes 时钟未统一 |
| P0-3 | P0 | 事务消息亲和性无可执行策略 |
| P0-4 | P0 | strict client 零失败缺真实证据 |
| P1-1 | P1 | steady lease 需要容量与重平衡数据 |
| P1-2 | P1 | ReceiptHandle 缺 bounded await/指标 |
| P1-3 | P1 | 双 readiness 不能绑定可逆 GOAWAY |
| P1-4 | P1 | admin blocking waiter 可饿死 health |
| P1-5 | P1 | 静态 executor/worker 缺 deadline stop |
| P1-6 | P1 | 最终 metrics 不能单独控制 rollout |
| P1-7 | P1 | Remoting 实现缝和 hot path 未明确 |
| P1-8 | P1 | bootstrap/HPA/PR 范围需收敛 |
| R1 | 优化 | 使用连接数 CV |
| R2 | 优化 | 单一预算 helper 和交叉校验 |
| R3 | 优化 | 触发源、阶段和 cutoff 可观测 |
| R4 | 优化 | 删除不存在的 gRPC 分批 GOAWAY |
| R5 | 优化 | 容量不等式替代魔法副本数 |
| R6 | 优化 | 零失败硬门禁加统计补充 |

## 8. 关键本地证据

- gRPC server ownership：`proxy/src/main/java/org/apache/rocketmq/proxy/grpc/GrpcServer.java`
- transaction heartbeat：`proxy/src/main/java/org/apache/rocketmq/proxy/service/transaction/ClusterTransactionService.java`
- transaction check routing：`proxy/src/main/java/org/apache/rocketmq/proxy/service/client/ProxyClientRemotingProcessor.java`
- ReceiptHandle cleanup：`proxy/src/main/java/org/apache/rocketmq/proxy/service/receipt/DefaultReceiptHandleManager.java`
- Remoting single replay：`remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingClient.java`
- Remoting server channels：`remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingServer.java`
- static monitor shutdown：`common/src/main/java/org/apache/rocketmq/common/thread/ThreadPoolMonitor.java`
- metrics shutdown：`proxy/src/main/java/org/apache/rocketmq/proxy/metrics/ProxyMetricsManager.java`
- Surefire/JUnit dependency baseline：`pom.xml`
- Helm probe/provider baseline（部署仓库）：`templates/proxy.yaml`、`values-production-aws-data.yaml`、`values-production-ack-data.yaml`
