# SendPermit —— 单次发送的双终态记账凭证

> 代码:`proxy/src/main/java/org/apache/rocketmq/proxy/lifecycle/SendPermit.java`
> 配图源文件:`diagrams/send-permit.puml`,渲染结果在 `diagrams/svg/sendpermit-*.{svg,png}`
> 本文只讲 SendPermit 及其直接协作者。整体状态机见 `state-transitions.md`。

## 1. 它解决什么问题

优雅下线的核心动作在 `ProxyLifecycleCoordinator`:

```java
transition(MIGRATING, DRAINING, "draining");
gate.closeAdmission();        // :325 —— 之后不再放行新的 send
return gate.drainedFuture();  // :326 —— 等在途 send 归零
```

`closeAdmission()` 之后不再发放新凭证,然后**等 `accepted` 计数归零**才认为「业务已排空」。所以每张已发放的凭证必须、且只能被释放一次:

- **漏放一次** → 计数永不归零 → QUIESCING/DRAINING 一直挂到硬超时被 `forceDrain` 强杀,优雅下线退化成强杀。
- **多放一次** → 计数提前归零 → 还在飞的请求被当成已完成,进程可能在 Broker 写入未落地时就被关掉。

`SendPermit` 就是这张凭证。它的唯一职责是:**判断一次发送到底彻底结束了没有,并在结束的那一刻恰好一次地把 `SendDrainGate` 的在途计数减 1**。

## 2. 为什么必须是「双终态」

一次 send 有两条**独立的**完成线,跑在不同线程、完成顺序不确定:

| 区 | 含义 | 谁来写 |
|---|---|---|
| **backend** | 到 Broker 的调用结束了吗? | worker 线程,`SendLifecycleMessagingActivity.java:74,81` |
| **protocol** | 面向客户端的 gRPC 流关闭了吗? | 传输层,经 `GrpcSendLifecycleHolder.onStreamClosed`(`:64-71`) |

只看其中任意一条都会出错:

- **只看 protocol**:客户端取消或断连时流立刻关闭,但 Broker 调用还在飞。此时就减计数等于「提前宣布排空」。
- **只看 backend**:请求在 dispatch 之前就被拒了(校验抛异常、闸门已关),压根没有 Broker 调用,backend 永远等不到终态。

所以 `maybeRelease()` 做的是两者的 **AND**(`SendPermit.java:135`)。

![SendPermit 双终态状态图](diagrams/svg/sendpermit-1-dual-terminal-state.svg)

图中那条水平虚线是并发分区分隔线:上半区 backend、下半区 protocol,各自独立推进,只有汇合才落到 RELEASED,再由 RELEASED 触发 `onRelease.run()` 去减闸门计数。

## 3. 三个实现细节

### 3.1 打包进一个 `AtomicInteger`

```java
// backend: bits 0-1
private static final int BACKEND_MASK        = 0b11;
private static final int BACKEND_NOT_STARTED = 0;
private static final int BACKEND_STARTED     = 1;
private static final int BACKEND_TERMINAL    = 2;
private static final int BACKEND_SKIPPED     = 3;
// protocol: bit 2
private static final int PROTOCOL_TERMINAL   = 1 << 2;
// released: bit 3
private static final int RELEASED            = 1 << 3;
```

两区状态 + released 标志共处一个整数(`:33-41`),于是「检查 AND 条件」和「抢占 released 标志」是**同一次 CAS 里的原子决策**(`:146-147`)。不存在「两个线程都看到两边已终态、于是各减一次」。只有赢下 `RELEASED` 位那次 CAS 的线程才会调 `onRelease.run()`。

### 3.2 backend 三个出口,前提各不相同

`backendStarted()`(`:71-82`)—— 赢 CAS 才返回 `true`,语义是「你可以去调 Broker 了」:

```java
if (!lifecycle.backendStarted()) {
    // Cancelled/skipped before dispatch: never call the Broker.
    return new CompletableFuture<>();
}
```

这就是 `SendLifecycleMessagingActivity.java:74` 那个判断的含义 —— 输了就返回一个永不完成的 future,绝不重复调 Broker(流已经关了,也没人在等这个响应)。

`backendTerminal(cause)`(`:85-102`)—— **只能从 STARTED 来**,从别的状态调会主动抛异常:

```java
if (b != BACKEND_STARTED) {
    throw new IllegalStateException(
        "backendTerminal is only valid from STARTED, current backend=" + b);
}
```

这是故意「响亮地失败」而非静默修补:状态机被写坏了必须暴露出来。

`tryBackendSkipped(reason)`(`:105-117`)—— **只允许 `NOT_STARTED → SKIPPED`**。已经 STARTED 就返回 `false`,因为 Broker 调用已经飞出去了,不能假装它没发生,必须等真实的 future。**竞争失败是正常结果,不是错误** —— 接口注释也是这么写的(`SendLifecycleContext.java:35`)。

### 3.3 `completionFuture()`

在 RELEASED 时完成(`:151`),给上层一个「这一笔彻底完了」的挂钩。

## 4. 三条真实路径

### 4.1 正常路径:backend 先终态,protocol 后到

![happy path 时序图](diagrams/svg/sendpermit-2-seq-happy-path.svg)

关键几步:

- 第 2 步 tracer 在**拦截器之前**就创建 holder 并 `openSendRpcs += 1`,通过 `filterContext` 注入 gRPC Context(`GrpcSendStreamTracerFactory.java:70-72`)。
- 第 6-7 步 `tryAcquire` 的成功 CAS 就是**准入线性化点**:计数先加,再造 permit(`SendDrainGate.java:63-65`)。
- 第 12 步 `SendLifecycleBindPipeline` 在**service 线程**上把 permit 拷到 `ProxyContext`。必须在这里做,因为 worker 线程看不到 gRPC Context。
- 第 20 步 backend 到 TERMINAL 时 `maybeRelease` 因 protocol 还 OPEN **不释放**。
- 第 23-24 步:tracer 关流时也会试一次 `tryBackendSkipped`,返回 `false` 是**预期行为**,不是错误。
- 第 26-28 步 protocol 终态到位,AND 满足 → RELEASED → 计数减 1;若此时闸门已关且计数归零,`drainedFuture` 完成。

### 4.2 跳过路径:同步校验在 dispatch 前抛异常

![sync validation skip 时序图](diagrams/svg/sendpermit-3-seq-sync-validation-skip.svg)

`GrpcMessagingApplication.addExecutor`(`:182-191`)先在 service 线程跑 pipeline + `validateContext()`,**再** `executor.submit()`。异常发生在 submit 之前,worker 永不运行,没人会调 `backendStarted()` —— backend 会永久停在 NOT_STARTED。

这正是 catch 块里 `markSendSkipped(context)`(`:262`、`:267-272`)存在的理由:

```java
private void markSendSkipped(ProxyContext context) {
    SendLifecycleContext lifecycle = context.getSendLifecycleContext();
    if (lifecycle != null) {
        lifecycle.tryBackendSkipped(SkipReason.SYNC_VALIDATION);
    }
}
```

它把 backend 推到 SKIPPED,补齐 AND 的另一半。用 `tryBackendSkipped` 而非直接赋值,是为了在「任务其实已 submit 成功且 worker 已 `backendStarted()`」的竞争下退化成无害的 no-op。放在 `writeResponse` 之前,是让 backend 终态先落地,随后 protocol 终态一到就立即释放。

`GrpcSendLifecycleHolder.onStreamClosed`(`:69`)里还有一次 `tryBackendSkipped(STREAM_CLOSED_BEFORE_DISPATCH)`,是同一个不变式的第二道兜底,覆盖「permit 已绑定但请求根本没走到 service 方法」的路径。两者叠加是有意的。

### 4.2.1 同一形状的第三处:线程池拒绝

`markSendSkipped` 在生产代码里有**三个**调用点,上面的 catch 块只是其中之一。第三处在 `GrpcTaskRejectedExecutionHandler.rejectedExecution`(`:523`):

```java
GrpcTask grpcTask = castGrpcTask(r);
if (grpcTask != null) {
    try {
        markSendSkipped(grpcTask.context);
        writeResponse(grpcTask.context, grpcTask.request, grpcTask.executeRejectResponse, ...);
```

这条路径与 4.2 的形状完全相同:`submit()` 本身成功了,但任务被队列满拒绝,`run()` 永不执行 → 没人调 `backendStarted()` → backend 永久停在 NOT_STARTED。所以拒绝处理器也必须补一次 SKIPPED。

这里有个坑值得记一笔:`submit()` 会把任务包进 `FutureTaskExt`,所以裸的 `instanceof GrpcTask` 永远不匹配 —— 必须经 `castGrpcTask` 解包(`:538-553`)。漏了这一步,拒绝路径上的 permit 就会静默泄漏。

注意这条路径复用了 `markSendSkipped`,因此上报的原因也是 `SYNC_VALIDATION`,而不是语义更贴切的 `EXECUTOR_REJECTED`。由于原因当前根本没被消费(见第 6 节),这不影响正确性。

### 4.3 无凭证路径:闸门已关

![gate closed 时序图](diagrams/svg/sendpermit-4-seq-gate-closed.svg)

`tryAcquire` 返回 `Optional.empty()`(`SendDrainGate.java:56-58`),**计数完全没动过**,也就没有任何东西需要释放。拦截器只在 holder 上记一个 `rejectBeforeAdmission(GATE_CLOSED)` 并以 `UNAVAILABLE` 关闭调用,业务 handler 从不被调用(`GrpcSendLifecycleInterceptor.java:76-83`)。

这条路对应 `SendLifecycleBindPipeline.java:39` 里 `holder.hasPermit()` 为 `false` 的分支 —— 不往 `ProxyContext` 写,下游看到 `getSendLifecycleContext() == null` 就完全不做生命周期记账。

`LATE_TRANSPORT`(连接在 lb cutoff 之后才建立,`:67-74`)走的是同一形状的路径。

## 5. 装配关系

`ProxyGracefulLifecycleWiring` 里 `SendDrainGate` 是单例(`:41`),同时交给拦截器(`:55`)和 coordinator(`:97`);`openSendRpcs` 计数器交给 tracer factory(`:54`)。

```
GrpcSendStreamTracerFactory ──创建──▶ GrpcSendLifecycleHolder ──存放──▶ SendPermit
          │                                   ▲                            ▲
       注入 gRPC Context                   bindPermit                   tryAcquire
          │                                   │                            │
          ▼                    GrpcSendLifecycleInterceptor ──────▶ SendDrainGate
   SendLifecycleBindPipeline                                              │
          │ 拷到 ProxyContext                                      closeAdmission /
          ▼                                                        drainedFuture
   SendLifecycleMessagingActivity ──backendStarted/backendTerminal──▶ SendPermit
```

注意 `openSendRpcs`(tracer 维护)和 `SendDrainGate.accepted`(闸门维护)是**两个不同的计数**:前者从 tracer 创建到流关闭,覆盖被拒和同步失败的 send;后者只统计真正被准入的 send,是 drain 判据。

## 6. 诚实标注:尚未接线的部分

- **`SkipReason` 目前只是形参**。`SendPermit.tryBackendSkipped` 和 `GrpcSendLifecycleHolder.rejectBeforeAdmission` 都没有把它存下来或打日志,因此「原因更精确」当前只体现在代码可读性上,运行时观测不到区别。
- **`SendProtocol.REMOTING` 未接线**。`SendPermit` 支持 `protocol()` 区分协议,但全仓库只有 gRPC 侧调用 `tryAcquire(SendProtocol.GRPC)`,Remoting 侧的 send 尚未纳入该闸门。
- **`SendPermit.protocol()` 无任何读者**(生产和测试都没有),它是为上一条的 Remoting 接入预留的。`GrpcSendLifecycleHolder.permit()` 则有生产读者,即 `SendLifecycleBindPipeline.java:40`。
