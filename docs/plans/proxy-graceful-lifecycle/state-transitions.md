# Proxy 优雅生命周期 —— 状态转移代码细节

> 代码:`proxy/src/main/java/org/apache/rocketmq/proxy/lifecycle/ProxyLifecycleCoordinator.java`
> 每条转移边下方直接贴触发它的**真实代码**及连带动作,而非概括。

## 1. 状态机总览

```
STARTING ──onStartupComplete──▶ READY
   │                      │
   │                      │ beginDrain() → orchestrate()
   │                      ▼
   │                   QUIESCING ──▶ MIGRATING ──▶ DRAINING ──▶ DRAINED ┄┄▶ (STOPPING → STOPPED, 未接线)
   │                      │            │            │
   └──────────────────────┴────────────┴────────────┴──▶ FORCE_DRAINING ┄┄▶ (STOPPING, 未接线)
        任一阶段遇异常/超时/STARTING-drain/TERM-before-ready
```

合法边表 `legalTransitions()`(`:89-105`),不在表中的转移直接抛异常:

```java
private static Map<ProxyLifecycleState, EnumSet<ProxyLifecycleState>> legalTransitions() {
    Map<ProxyLifecycleState, EnumSet<ProxyLifecycleState>> m = new EnumMap<>(ProxyLifecycleState.class);
    m.put(STARTING,       EnumSet.of(READY, QUIESCING, FORCE_DRAINING));
    m.put(READY,          EnumSet.of(QUIESCING, FORCE_DRAINING));
    m.put(QUIESCING,      EnumSet.of(MIGRATING, FORCE_DRAINING));
    m.put(MIGRATING,      EnumSet.of(DRAINING, FORCE_DRAINING));
    m.put(DRAINING,       EnumSet.of(DRAINED, FORCE_DRAINING));
    m.put(DRAINED,        EnumSet.of(STOPPING));
    m.put(FORCE_DRAINING, EnumSet.of(STOPPING));
    m.put(STOPPING,       EnumSet.of(STOPPED));
    return m;
}
```

## 2. 唯一转移原语 `transition(expected, next, reason)`(`:124-134`)

**所有**状态变更都必须经过它。它是唯一写 `state` 的地方:

```java
boolean transition(ProxyLifecycleState expected, ProxyLifecycleState next, String reason) {
    EnumSet<ProxyLifecycleState> allowed = LEGAL.get(expected);
    if (allowed == null || !allowed.contains(next)) {
        throw new IllegalStateException("illegal lifecycle transition " + expected + " -> " + next);
    }
    if (state.compareAndSet(expected, next)) {   // ← 线性化点:只有一个线程能赢
        reasonRef.set(reason);
        return true;
    }
    return false;   // ← CAS 输了:状态已被并发推进,不是错误
}
```

- **第 1 步** 校验边合法性 → 非法边 fail-loud。
- **第 2 步** `compareAndSet(expected, next)`:并发下只有从 `expected` 出发的那个线程成功。
- 赢家写入 `reason`(供 `/state` 和日志),返回 true;输家返回 false,调用方据此知道「这条边我没走成」。

## 3. `STARTING → READY`

唯一公开触发方法是 `onStartupComplete()`:

```java
public void onStartupComplete() {
    Throwable fatal = fatalRef.get();
    if (fatal != null) {
        throw new IllegalStateException("cannot become ready after a fatal startup failure", fatal);
    }
    if (!transition(ProxyLifecycleState.STARTING, ProxyLifecycleState.READY, "ready")) {
        throw new IllegalStateException("cannot complete startup from lifecycle state " + state.get());
    }
}
```

生产调用者是 `ProxyStartup.startAndSignalReady(...)`:先完整执行
`PROXY_START_AND_SHUTDOWN.start()`,只有正常返回才调用 `onStartupComplete()`。任一组件启动异常都会直接向外传播,coordinator 保持 STARTING,不会短暂发布 READY。

```java
static void startAndSignalReady(StartAndShutdown startup,
    ProxyLifecycleCoordinator coordinator) throws Exception {
    startup.start();
    if (coordinator != null) {
        coordinator.onStartupComplete();
    }
}
```

`/started` 与 `/ready` 现在由同一条成功边原子发布；STARTING 时两者都为 false,进入 READY 后都为 true,不存在只 started 但未 ready 的中间发布窗口。

## 4. 正常排空链 —— 全部在 `orchestrate(run)` 内(`:216-259`)

入口 `beginDrain(trigger)`(`:168-184`)用单个 `AtomicReference<DrainRun>` CAS 发布,赢家把 `orchestrate` 提交给调度器。以下是 `orchestrate` 全文,四条边(粗体注释)都在其中:

```java
private void orchestrate(DrainRun run) {
    DrainSession session = run.session();
    CompletableFuture<DrainResult> future = run.drainFuture();
    try {
        if (state.get() == ProxyLifecycleState.STARTING) {
            // 从未服务过业务:直接强制,不走正常链
            forceDrain(run, "starting_state", null);
            return;
        }

        // ── 边①  READY → QUIESCING ──
        transition(ProxyLifecycleState.READY, ProxyLifecycleState.QUIESCING, "quiescing");
        if (readinessWithdraw != null) {
            readinessWithdraw.run();          // 可选钩子(当前生产接线传 null)
        }
        // 停在 QUIESCING 直到 lbCutoff:等 provider(EndpointSlice + NLB target
        // deregistration)真正停止把新连接投给本 Pod,之后才允许迁移。
        lbTimer = scheduler.schedule(() -> continueToMigrating(run),
            Math.max(0L, session.lbCutoffNanos() - nanoClock.getAsLong()));
        scheduleHardDeadline(run, session);   // 兜底定时器
    } catch (Throwable t) {
        forceDrain(run, "orchestrate_exception", t);
    }
}

// lbCutoff 到点后由调度器回调执行的后半段
private void continueToMigrating(DrainRun run) {
    DrainSession session = run.session();
    CompletableFuture<DrainResult> future = run.drainFuture();
    try {
        // ── 边②  QUIESCING → MIGRATING ──
        if (!transition(ProxyLifecycleState.QUIESCING, ProxyLifecycleState.MIGRATING, "migrating")) {
            return;                           // 已被 forced / direct-TERM 抢占
        }
        for (DrainProtocolAdapter adapter : adapters) {
            adapter.startMigration();         // migration_started:gate 仍开放
        }

        CompletableFuture<Void> allNoNewWork = allOf(adapters, DrainProtocolAdapter::noNewWorkReached);
        allNoNewWork
            .thenCompose(ignored -> {
                // ── 边③  MIGRATING → DRAINING ──
                transition(ProxyLifecycleState.MIGRATING, ProxyLifecycleState.DRAINING, "draining");
                gate.closeAdmission();        // admission_closed:正常路径关闸点
                return gate.drainedFuture();  // 等 accepted-inflight 归零
            })
            .thenComposeAsync(ignored -> {
                ShutdownDeadline deadline = effectiveDrainDeadline(session);
                return allOf(adapters, adapter -> adapter.awaitTerminated(deadline));
            }, scheduler::execute)
            .whenComplete((ignored, error) -> {
                if (error != null) {
                    forceDrain(run, "drain_error", error);   // 任一阶段异常 → 强制
                    return;
                }
                // ── 边④  DRAINING → DRAINED ──
                if (transition(ProxyLifecycleState.DRAINING, ProxyLifecycleState.DRAINED, "drained")) {
                    future.complete(DrainResult.drained());
                }
            });
    } catch (Throwable t) {
        forceDrain(run, "migrate_exception", t);
    }
}
```

逐边说明:

| 边 | 所在方法 | reason | 这条线**具体做的事** |
|---|---|---|---|
| ① `READY→QUIESCING` | `orchestrate` | `quiescing` | 只做两件事:改状态;调可选的 `readinessWithdraw` 钩子(**当前生产接线传的是 `null`,所以是 no-op**)。`/ready` 转 503 是**间接**发生的——`isReady()` 要求 `state==READY`,状态一旦离开 READY 就返回 false。随后**注册 `lbTimer`,在 QUIESCING 停留 `lbDetachTimeoutSeconds`(默认 60s)**,给 EndpointSlice/NLB 摘流留时间;gate 仍开放,存量连接照常。 |
| ② `QUIESCING→MIGRATING` | `continueToMigrating`(由 `lbTimer` 回调) | `migrating` | **只有 lbCutoff 到点后才执行**。`transition` 返回 false 表示已被 forced/direct-TERM 抢占,直接返回。否则遍历每个 adapter 调 `startMigration()`:gRPC 侧即 `initiateServerDrain()` → `Server.shutdown()` → grpc-java 内建双 GOAWAY。**gate 依然开放**。 |
| ③ `MIGRATING→DRAINING` | `continueToMigrating` | `draining` | 在 `allNoNewWork`(所有 adapter 的 `noNewWorkReached` 都完成)之后触发;紧接着 `gate.closeAdmission()`——**正常排空路径的关闸线性化点**,此后新 send 被拒;再 `return gate.drainedFuture()` 等已接纳的 send 双终态归零。强制路径也会在 force adapter 之前幂等关同一扇闸。 |
| ④ `DRAINING→DRAINED` | `continueToMigrating` | `drained` | 在 gate 归零 **且** 每个 adapter 的 `awaitTerminated(deadline)` 都成功后;`transition` 成功则 `future.complete(DrainResult.drained())`,PreStop 的 `.join()` 得以返回。若 `error!=null` 则改走 `forceDrain(run,"drain_error",...)`。 |

### 为什么 ① 必须等待,而不能立刻迁移

摘流是 **kubelet/EndpointSlice + NLB target deregistration** 的行为,Proxy **无法直接观测**「我的 IP 已被摘掉」。因此设计上不观测、改用固定时间等待:`T0 + lbDetachTimeoutSeconds` 作为 `lbCutoff`。若在摘流完成前就发 GOAWAY,客户端断开重连**可能又落回本 Pod**,迁移空转。

第二道防线是 late-transport 标记:`GrpcTransportLifecycleFilter` 给 `lbCutoff` 之后新建的 transport 打 `LATE_AFTER_LB_CUTOFF`,其 send 被 interceptor 拒绝(视为 provider 摘流违约)。

> `lbDetachTimeoutSeconds` 的 60s 默认值需用真实环境实测「readiness 失败后最后一个新连接到达」的 p999 来校准 —— 这属于计划文档 §7 Step 1 的 provider spike,本地测试无法替代。

`effectiveDrainDeadline`(`:200-207`)决定 `awaitTerminated` 用哪个 deadline——若存在更紧的 `stopOverride`(direct-TERM 设的)就用它,否则用 `session.hardDeadlineNanos()`:

```java
private ShutdownDeadline effectiveDrainDeadline(DrainSession session) {
    ShutdownDeadline override = stopOverride.get();
    long hard = session.hardDeadlineNanos();
    if (override != null && override.deadlineNanos() - hard < 0L) {
        return override;      // 取更早(更紧)的那个
    }
    return new ShutdownDeadline(hard, nanoClock);
}
```

## 4.1 向下钻:每条边在协议层到底调了什么

上面 `orchestrate` 里只看到 `adapter.startMigration()` / `awaitTerminated()` / `force()`。它们在 gRPC 侧的落地实现在
`lifecycle/grpc/GrpcDrainAdapter.java` 与 `grpc/GrpcServer.java`。调用链总览:

```
边② MIGRATING   → adapter.startMigration()  → GrpcServer.initiateServerDrain() → io.grpc.Server.shutdown()   ← 是的,migration 就是调 shutdown()
边③ DRAINING    → gate.closeAdmission()（应用层关闸,不碰 gRPC）
边④ DRAINED 前  → adapter.awaitTerminated() → registry.closeAll() + GrpcServer.awaitServerTermination()
强制 FORCE_*    → adapter.force()           → registry.closeAll() + GrpcServer.forceServerShutdown() → Server.shutdownNow()
```

### 边②的落地:`startMigration()`(`GrpcDrainAdapter.java:66-73`)

```java
@Override
public void startMigration() {
    if (migrationStarted.compareAndSet(false, true)) {   // once-only
        server.initiateServerDrain();
        // The built-in double GOAWAY is the no-new-work boundary for the primary spike outcome.
        noNewWork.complete(null);                        // 立即满足 no-new-work
    }
}
```

再往下 `GrpcServer.initiateServerDrain()`(`GrpcServer.java:90-95`):

```java
/** Once-only, non-blocking ordered shutdown; triggers grpc-java's built-in double GOAWAY. */
public void initiateServerDrain() {
    if (serverDrainStarted.compareAndSet(false, true)) {
        server.shutdown();          // ← io.grpc.Server.shutdown()
    }
}
```

**这里是关键、也最容易误解的一点**:`io.grpc.Server.shutdown()` 在 **MIGRATING 边**就被调用,而不是在最后拆除时。原因是它:

- **非阻塞**,立即返回;
- **不杀现存 stream**,只停止接受新 stream;
- 触发 grpc-java 内建的**双 GOAWAY** 协议(`GOAWAY(lastStreamId=MAX)` → `PING` → `PING_ACK`/10s 兜底 → `GOAWAY(lastStreamCreated)`),让客户端去别的 Pod 重连。

所以它语义上是「迁移信号」,不是「关服务器」。真正的强杀是后面的 `shutdownNow()`。

也正因为双 GOAWAY 一发出就构成了 no-new-work 边界,`startMigration()` 里紧跟着 `noNewWork.complete(null)` —— 这就是为什么 gRPC-only 实现下 `orchestrate` 的 `allNoNewWork` 会立刻完成,不需要等 `migrationCutoff`。

### 常见疑问:migration 已经 `shutdown()` 了,DRAINING 还关闸不是冗余吗?

不冗余。三件事分处三层,谁也替代不了谁:

| 层 | 手段 | 作用对象 | 性质 | 能挡住漏网 send 吗 | 知道 Broker 写完没 |
|---|---|---|---|---|---|
| 传输层 | `server.shutdown()`(边②) | TCP 连接 / HTTP2 流 | **异步、建议式**(靠客户端处理 GOAWAY) | 挡不住 GOAWAY 窗口内到达的 | 不知道,只看流是否关闭 |
| 准入层 | `gate.closeAdmission()`(边③) | 每次 SendMessage RPC | **同步、线性化**(关闸后 `tryAcquire` 立即返回 empty) | 能,interceptor 直接 `UNAVAILABLE` 拒掉 | —— |
| 记账层 | `SendPermit` 双终态 | 已准入的单次 send | backend + protocol 两终态 AND | —— | 知道,靠 backend 终态 |

关键在于 `initiateServerDrain()` 调的是 `io.grpc.Server.shutdown()` 而非 `shutdownNow()`。`shutdown()` 只做两件事:不再接受**新连接**、对现存连接发 GOAWAY。GOAWAY 是异步的、对客户端是"建议",不是一道瞬时硬墙——**现存连接上已在飞、以及客户端处理完 GOAWAY 之前又发来的 SendMessage,仍会正常派发到 handler**。也就是说 `shutdown()` 之后存在一个 **GOAWAY 窗口**,期间老连接上的 send 照样到达拦截器。

这正是设计上刻意把 migration 和 draining 分成两步的原因:MIGRATING 阶段**故意让老连接继续正常收发**,给客户端和 LB 时间把连接迁走,窗口内落到本 Pod 的 send 必须照常准入、照常打到 Broker——这才是"优雅"迁移。等到 no-new-work + lbCutoff 都满足、确定不会再有正常新业务之后,才在边③把准入层这道硬墙立起来(`closeAdmission`),挡下 GOAWAY 窗口里仍在漏进来的 send。若 migration 一开始就关闸,迁移窗口里的合法 send 会被误拒。

进一步说,边④的 `awaitServerTermination`(等**传输层**所有 HTTP2 流关闭)也替代不了边③的 `gate.drainedFuture()`(等**应用层**每张 permit 双终态)。二者不等价:一个 send 的 HTTP2 流可以已经关闭(protocol terminal),但发往 Broker 的写还在飞(backend 未 terminal)。所以必须**先**等闸门计数归零(③)**再**等 server 终止(④),否则可能在 Broker 写入尚未落地时就放行进程停止。SendPermit 的双终态存在,就是为了给出这个准确的排空时机。

### 边④的落地:`awaitTerminated(deadline)`(`GrpcDrainAdapter.java:80-99`)

```java
@Override
public CompletableFuture<Void> awaitTerminated(ShutdownDeadline effectiveDeadline) {
    registry.closeAll(policy);                       // ① 主动结束非 unary 流
    return registry.drainedFuture().thenComposeAsync(ignored -> {   // ② 等这些流真正 terminal
        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            if (server.awaitServerTermination(effectiveDeadline)) {   // ③ 等 Server 终止
                done.complete(null);
            } else {
                server.forceServerShutdown();                        // ④ 超时 → 强杀
                done.completeExceptionally(new IllegalStateException("server_not_terminated"));
            }
        } catch (InterruptedException e) {
            server.forceServerShutdown();
            done.completeExceptionally(e);
            Thread.currentThread().interrupt();
        }
        return done;
    }, terminationExecutor);
}
```

① 的 `registry.closeAll(policy)` 按 RPC 类型选 close status(`GrpcDrainStatusPolicy`):Telemetry 用 `Status.OK`(让 5.0.7/5.2.1 客户端走静默的 1 秒 observer renewal),其余非 unary(ReceiveMessage/PullMessage/未来新流)用 `UNAVAILABLE("[PROXY_DRAINING] reconnect")`,避免把被截断的业务流伪装成成功。
registry 的注册计数 CAS 与 `live` 集合发布必须和 `closeAll` 互斥:否则
`register` 可能已经把 open count 加 1、却还没执行 `live.put`,此时
`closeAll` 会永久漏掉这条调用。实现用短 `registrationLock` 把
“确认未关闭 → count++ → live.put”与“置 closed bit → snapshot live”
线性化；锁外再逐一发送 close intent,避免业务 terminal 回调在锁内重入。
若注册已经关闭,interceptor 会按同一 policy 直接关闭调用并且绝不调用
业务 handler。
这个 continuation 必须提交给由 `ProxyStartup` 显式创建和托管的
`ProxyLifecycleTerminator` executor；不能在完成最后一张 permit 的 gRPC tracer 线程上内联阻塞。lifecycle scheduler 与 terminator 相互独立,所以 server wait 卡住时 hard-deadline timer 仍可执行 force。

③ 的 `awaitServerTermination`(`GrpcServer.java:97-103`)只消费 deadline 剩余时间,返回原始 boolean:

```java
public boolean awaitServerTermination(ShutdownDeadline deadline) throws InterruptedException {
    long remainingNanos = deadline.remainingNanos();
    return remainingNanos > 0
        ? server.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)
        : server.isTerminated();     // deadline 已耗尽:不阻塞,只报当前状态
}
```

注意 ④ 返回的是 **exceptionally 完成**的 future → 回到 `orchestrate` 的 `.whenComplete` 里 `error != null` → 走 `forceDrain(run, "drain_error", error)` → 状态变 `FORCE_DRAINING`,而不是 `DRAINED`。

### 强制分支的落地:`force(deadline)`(`GrpcDrainAdapter.java:101-107`)

```java
@Override
public void force(ShutdownDeadline effectiveDeadline) {
    if (forced.compareAndSet(false, true)) {   // once-only
        registry.closeAll(policy);
        server.forceServerShutdown();
    }
}
```

`GrpcServer.forceServerShutdown()`(`GrpcServer.java:105-110`)才是真正的强杀:

```java
public void forceServerShutdown() {
    if (forceStarted.compareAndSet(false, true)) {
        server.shutdownNow();       // ← 取消所有剩余 stream
    }
}
```

### 进程停止阶段(SIGTERM,在 coordinator 之外)

`ProxyStartup` 的 shutdown hook 在 `beginDrain(...).drainFuture()` 有界 join 之后,再调
`GrpcServer.shutdownOwnedResources(stopDeadline)`(`GrpcServer.java:120`),用共享 stop deadline 做有界四段式收尾:
`initiateServerDrain() → awaitServerTermination() →(失败)forceServerShutdown() → 再 await 一次 →(仍失败)记 server_not_terminated`,
最后 `finally` 里 `unregisterTlsListener()`。它不会无界阻塞 hook。

## 5. `* → FORCE_DRAINING` —— `forceDrain(run, reason, cause)`(`:270-290`)

任一状态(除已终态)遇异常/超时时的强制分支。全文:

```java
private void forceDrain(DrainRun run, String reason, Throwable cause) {
    if (!tryEnterForceDraining(state::get, state::compareAndSet)) {
        return;                                    // 已强制/排空/停止
    }
    gate.closeAdmission();                         // 先关闸,再做 best-effort force
    reasonRef.set("forced:" + reason);
    ShutdownDeadline deadline = effectiveDrainDeadline(run.session());
    List<Throwable> causes = new ArrayList<>();
    if (cause != null) {
        causes.add(cause);
    }
    for (DrainProtocolAdapter adapter : adapters) {
        try {
            adapter.force(deadline);               // best-effort:单个失败不影响其余
        } catch (Throwable forceError) {
            causes.add(forceError);                 // 保留异常,继续 force 其余 adapter
        }
    }
    run.drainFuture().complete(DrainResult.forced(reason, causes));
}
```

`tryEnterForceDraining` 会在 CAS 失败后**重读并重试**,只在观察到
`FORCE_DRAINING`、`DRAINED`、`STOPPING` 或 `STOPPED` 时退出。不能把一次
CAS 失败解释成“别的线程已经 force”:正常线程可能只是把
`MIGRATING → DRAINING`,而 hard-deadline 的一次性 timer 已经被消费；若此时
直接返回,后续 gate/transport 卡住会让 drain future 永不完成。

这条线**具体做的事**:循环 CAS 到 `FORCE_DRAINING`(**不经过** `transition()`,因为源状态可能正在沿正常链前进,合法边表已覆盖 STARTING/READY/QUIESCING/MIGRATING/DRAINING→FORCE_DRAINING);CAS 赢家立即关闭 shared gate,确保任何 adapter force 之前已停止新准入;写 `reason="forced:..."`;对每个 adapter 调 `force(deadline)`(gRPC 侧 = `forceServerShutdown()` → `Server.shutdownNow()`),单个 adapter 抛错会被收进 causes 且不妨碍其余 adapter;最后用 `DrainResult.forced` 完成 future,让等待方(PreStop join)得到「强制」结果而非正常 drained。

五个触发点(reason 后缀 → 场景):

| reason | 代码行 | 场景 |
|---|---|---|
| `starting_state` | `:220-223` | `orchestrate` 开头发现仍在 STARTING —— 从未服务业务,跳过正常链直接强制 |
| `orchestrate_exception` | `:256-258` | 编排主体抛异常(catch 兜底) |
| `drain_error` | `:245-248` | `closeAdmission`/`awaitTerminated` 阶段的 future 异常完成 |
| `hard_deadline_exceeded` | `:261-267` | 见下方 `scheduleHardDeadline` |
| `term_before_ready` | `:318-321` | direct-TERM 在 STARTING 阶段到达 |

兜底定时器 `scheduleHardDeadline`(`:261-268`):

```java
private void scheduleHardDeadline(DrainRun run, DrainSession session) {
    long delay = session.hardDeadlineNanos() - nanoClock.getAsLong();
    migrationTimer = scheduler.schedule(() -> {
        if (!run.drainFuture().isDone()) {              // drain 还没完成
            forceDrain(run, "hard_deadline_exceeded", null);
        }
    }, Math.max(0L, delay));
}
```

在 `orchestrate` 尾部注册(`:255`)。到点时若 `drainFuture` 仍未完成,就强制收尾——这是整条正常链的时间上界保险。

## 6. direct-TERM 抢占 —— `escalateForStop(stopDeadline)`(`:308-336`)

绕过 lb/migration cutoff 的等待,立即冻结。全文:

```java
CompletableFuture<Void> escalateForStop(ShutdownDeadline stopDeadline) {
    stopOverride.set(stopDeadline);          // 记录更紧的 deadline,影响 effectiveDrainDeadline
    phaseGeneration.incrementAndGet();
    cancel(lbTimer);                         // 取消未触发的阶段定时器
    cancel(migrationTimer);
    DrainRun run = drainRunRef.get();
    CompletableFuture<Void> issued = new CompletableFuture<>();
    scheduler.execute(() -> {
        try {
            ProxyLifecycleState cur = state.get();
            if (cur == ProxyLifecycleState.STARTING) {
                if (run != null) {
                    forceDrain(run, "term_before_ready", null);
                }
                issued.complete(null);
                return;
            }
            gate.closeAdmission();           // 立即关闸,不等 no-new-work
            for (DrainProtocolAdapter adapter : adapters) {
                adapter.startMigration();
                adapter.force(stopDeadline); // 立即冻结每个协议
            }
            issued.complete(null);
        } catch (Throwable t) {
            issued.completeExceptionally(t);
        }
    });
    return issued;
}
```

与正常链的区别:正常链**先迁移、等 no-new-work、再关闸**;抢占路径**直接关闸 + 立即 force**,并用 `stopOverride` 把后续 `awaitTerminated` 的 deadline 收紧到 TERM 给的短预算。返回的 `issued` future 在「两个 freeze 都已发出」后完成,供调用方证明「先冻结再等待」。

## 7. 尚未接线的落差(诚实标注)

合法边表中定义、但**当前生产代码未驱动**的边(与 `proxyAdminPort`/`migrationCutoff` 同类的「已定义未接线」):

- **`DRAINED → STOPPING`、`STOPPING → STOPPED`**(`:101-103` 表中存在):全仓库**无任何** `transition(..., STOPPING, ...)` / `transition(..., STOPPED, ...)` 调用。`ProxyRuntime` 注释称「drives STOPPING/STOPPED」但未实现——SIGTERM 实走 `beginDrain().join()` + 旧 `PROXY_START_AND_SHUTDOWN.shutdown()`,coordinator 停在 `DRAINED`/`FORCE_DRAINING`。
- **`escalateForStop`**:coordinator 内已实现,但**无生产调用者**(仅单测)。当前 SIGTERM 路径在 `ProxyStartup` 直接 `beginDrain(SIGTERM_FALLBACK)`,未走抢占语义。

因此实际可观察轨迹:
`STARTING → READY →(drain)→ QUIESCING → MIGRATING → DRAINING → DRAINED`,异常/超时为 `… → FORCE_DRAINING`。`STOPPING`/`STOPPED` 是预留终态,进程实际停止由旧静态关闭链代劳。

## 8. 不改状态的辅助判据(供探针读取)

| 方法 | 行 | 逻辑 |
|---|---|---|
| `isStarted()` | `:205-208` | 状态不再是 STARTING |
| `isLive()` | `:147-150` | 无 fatal 且非 STOPPED |
| `isReady()` | `:152-156` | 无 fatal 且状态恰为 READY |
| `markFatal(component, cause)` | `:136-140` | 只 CAS 写 `fatalRef` + reason,**不改状态**;经 `isLive/isReady` 间接影响探针 |
| `snapshot()` | `:230-243` | 打包 state + reason + drainId + sticky forced outcome + gate 计数,供 `/state`;forced drain 即使随后推进 STOPPING/STOPPED 仍保持 `forced=true` |
