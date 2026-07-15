# Findings: RocketMQ Proxy Graceful Lifecycle

## Repository Facts

- `ProxyStartup` starts components in registration order and shuts them down in reverse order through `AbstractStartAndShutdown`.
- gRPC shutdown invokes `Server.shutdown().awaitTermination(timeout)` with a default 30-second timeout, but it does not force-stop after timeout or explicitly report whether in-flight work drained.
- `GrpcMessagingApplication.shutdown()` calls `shutdown()` on executors without awaiting completion.
- Remoting already has a shutdown gate: when `isShuttingDown` is true, clients newer than 5.3.1 receive `GO_AWAY`; the Java Remoting client reconnects and transparently retries, and producer retry codes include `GO_AWAY`.
- Proxy creates a default `NettyServerConfig` but does not enable its graceful shutdown flag, so the existing Remoting GO_AWAY window is not activated for Proxy.
- Remoting graceful shutdown is currently time-based sleep, not readiness withdrawal plus exact in-flight drain.
- Remoting send processing forwards synchronously with a three-second Proxy-to-Broker request timeout; gRPC send completion is asynchronous through a `CompletableFuture`.
- There is no unified READY/DRAINING state, readiness endpoint, or send in-flight metric in the Proxy module.
- This repository has no Kubernetes/Helm deployment manifest to coordinate `preStop`, readiness, PDB, or rolling-update budgets.
- The Docker entrypoint uses `exec ./mqproxy`, but `distribution/bin/mqproxy` starts `runserver.sh` without `exec`, and both stock and Docker-customized `runserver` scripts start Java without `exec`; TERM sent to container PID 1 is therefore not guaranteed to reach the Proxy JVM.

## Java Client Facts

- The Remoting Java client supports GO_AWAY reconnection by default, but the first request observing GO_AWAY pays a reconnect/retry cost unless a standby connection is already warm.
- The RocketMQ 5.x Java gRPC client creates one `ManagedChannel` per endpoint set, disables gRPC automatic retry, and relies on producer-level immediate retries across route candidates.
- With a single Kubernetes Service VIP and long-lived channels, scale-out does not naturally rebalance existing connections to new Pods.
- `rocketmq-client-java` 5.0.7 and 5.2.1 both embed gRPC Java 1.50, retain the same single-Channel/`pick_first`/Producer-retry behavior, and default to three Producer attempts with a three-second RPC timeout.
- Version 5.2.1 adds 300-second keepalive, 30-second keepalive timeout, and keepalive-without-calls. Its `ReconnectEndpointsCommand` handler only flips a boolean and has no operational Channel close/rebuild path.
- The official classic `rocketmq-client` release line has no 5.2.1 artifact. Client 5.2.0 contains GO_AWAY reconnect/transparent-retry code, but the current server gate only emits GO_AWAY for versions newer than 5.3.1; strict Remoting coverage must therefore start at 5.3.2.

## External Source Notes

- Apache RocketMQ issue 7330 and PR 7467 introduced GO_AWAY, reconnection, transparent retry, and a shutdown wait specifically for graceful shutdown.
- The current RocketMQ 5.x clients repository remains gRPC/protobuf based and the Java client release line is separate from this server repository.
- Kubernetes marks terminating Pod endpoints `ready=false` before container exit, while `PreStop` must finish before TERM and consumes the same termination grace-period budget.
- Kubernetes supports stable gRPC health probes, and a zero-unavailable rolling update requires `maxUnavailable: 0` together with nonzero `maxSurge`.
- Current AWS Load Balancer Controller documentation supports `service.beta.kubernetes.io/aws-load-balancer-target-group-attributes` with both `deregistration_delay.timeout_seconds` and `deregistration_delay.connection_termination.enabled`; the chart's AWS production values currently set neither.
- Current Alibaba Cloud NLB documentation says unhealthy/removed backends stop receiving new requests while existing sessions can continue during the configured connection-drain timeout. ACK guidance explicitly pairs connection drain with Pod `preStop` and readiness, so the existing 30-second annotation alone is insufficient while Proxy readiness remains TCP-only and PreStop immediately shuts it down.

## Design Implications

- A server-only sleep cannot meet the strict latency target. With immutable SDKs, readiness withdrawal, standard protocol GO_AWAY, periodic randomized connection leases, exact in-flight tracking, and a long Kubernetes drain window must cooperate.
- Planned termination can be made effectively invisible, but SIGKILL, OOM, node loss, and one-way sends cannot receive the same guarantee.
- To avoid ambiguous retry duplicates, admitted sends must be allowed to return their Broker result before transport/process teardown.
- A short `PreStop` propagation wait is sufficient for planned Pod deletion; the process-level drain must still handle direct SIGTERM idempotently.
- gRPC connection age is the only available server-side mechanism that routinely breaks multi-year Telemetry streams without SDK changes. Its two-GOAWAY algorithm preserves active streams, while the Producer retry layer covers the remaining race window.
- Remoting requires a Proxy-specific per-Channel lease because the existing global `isShuttingDown` gate only runs during final server shutdown.
- The first rollout from old Proxy binaries cannot inherit the new guarantee: old connections have neither leases nor drain state. Strict SLO starts only after all Pods run the lifecycle version and one complete lease period has elapsed.

## Current-State Problem Inventory

- P0: the process launch chain does not `exec` through to Java, so container `SIGTERM` may never reach the JVM shutdown hook.
- P0: Proxy has no shared lifecycle/admission state; `preShutdown()` has no coordinated implementation and readiness cannot be withdrawn before send admission closes.
- P0: neither protocol has an exact send drain barrier covering both Broker completion and client-response terminal completion, so shutdown can race accepted sends.
- P0: gRPC ignores the boolean result of `awaitTermination`, has no forced-stop fallback, leaves long-lived telemetry streams unmanaged, and shuts executors without awaiting them.
- P0: Remoting graceful shutdown is disabled by default in Proxy, and the existing implementation is a fixed sleep plus `GO_AWAY` only for newer clients rather than exact drain.
- P1: persistent client connections are not proactively migrated or rebalanced; scale-out can add healthy Pods without moving existing send traffic to them.
- P1: startup lacks a dependency/warmup readiness gate, so a cold Proxy may receive traffic before route caches and Broker connections are ready.
- P1: stable advertised access addresses and distinct standby Proxy instances are not enforced, so reconnect may return to the terminating Pod or the same backend.
- P1: component-local waits do not share an absolute shutdown deadline, and some executors/timers are not awaited or visibly closed.
- P1: the repository provides no Kubernetes lifecycle contract for readiness, `preStop`, rollout surge/unavailability, PDB, or HPA scale-down pacing.
- P2: lifecycle state, exact in-flight sends, pending response writes, drain notifications, capability ratios, and forced shutdowns are not observable.
- Boundary: acknowledged sends may still have an unknown result if the Broker persists before the Proxy response is lost; `sendOneway` and Local mode cannot share the strict guarantee.

## Adversarial Review Findings

- The lifecycle gate must linearize admission closure and in-flight accounting; a double-read counter alone can let shutdown observe zero before a late reject response is flushed.
- gRPC application rejection metadata cannot be relied on after `Server.shutdown()` has already rejected new streams at the transport layer.
- gRPC `ServerCall.close` alone is not proof that the Broker future completed or response bytes flushed; these are separate completion conditions.
- Strict Remoting guarantees require distinguishing drain-notice-capable, GO_AWAY-only, and fully legacy clients and enforcing a minimum supported version.
- Two connections through one Service VIP do not guarantee two Proxy instances; strict handoff needs observable instance identity plus distinct-backend enforcement or endpoint-aware resolution.
- EndpointSlice update, PreStop, and TERM are concurrent control-plane/node actions rather than a portable total order; PreStop must actively begin an idempotent quiesce instead of only sleeping.
- PDB does not constrain every deletion path, HPA and GitOps replica ownership must be singular, and external LB deregistration behavior must be measured for the concrete environment.
- A zero-failure sample is statistical evidence, not a mathematical guarantee; acceptance reporting must state sample size, repetitions, confidence bound, and an explicit observation window.
- The image launch chain itself is part of graceful shutdown correctness and must be changed to `exec` through to Java or use a proven signal-forwarding init.
- Remoting admission cannot begin in `AbstractRemotingActivity`: `NettyRemotingAbstract` creates and submits a `RequestTask` first, so a queued task could otherwise appear after drain observed zero. The admission permit must be acquired before executor submission and carried by the internal task/dispatch context.
- `RequestTask` already owns the request, channel, and dispatch runnable, making an explicit non-wire `RequestAdmissionContext` safer than a channel/opaque side map; executor rejection, stopped tasks, and channel close can then terminate the same once-only permit without opaque-collision cleanup races.
- `AbstractRemotingActivity` currently starts the Broker future asynchronously and calls `ctx.writeAndFlush(response)` without retaining the `ChannelFuture`; exact drain therefore requires separate Broker-terminal and RPC/write-terminal signals, including non-writable and write-failure branches.
- gRPC call/stream terminal callbacks are application-level completion evidence, not proof that bytes left Netty. Final safety requires closing transport intake and checking bounded `Server.awaitTermination`; a false result must be visible and trigger forceful termination.
- `NettyRemotingAbstract.processRequestCommand` currently creates the processing runnable, checks shutdown/flow control, constructs `RequestTask`, and submits it; an optional lifecycle hook can therefore acquire before submission and pass an explicit context with the task without changing any wire protocol.
- Core Remoting `writeResponse` already accepts a callback invoked from the Netty `ChannelFuture`, so GO_AWAY, executor-reject, and other pre-dispatch responses can share exact pending-write accounting; the Proxy activity writer must be refactored onto the same terminal contract.
- A long `PreStop` with `maxSurge: 1/maxUnavailable: 0` does not by itself serialize terminating old Pods. Strict rollout needs `minReadySeconds` longer than the full Pod termination bound plus frozen HPA scale-down, or an explicit rollout coordinator; otherwise several old Pods may drain concurrently.
- The first lifecycle rollout needs two stages because an old image has no port 8082: deploy the capable binary with lifecycle disabled and legacy probes first, then enable lifecycle and switch to HTTP probes only after every Pod runs the capable image.
- Current Kubernetes Deployment documentation defines `minReadySeconds` as the delay before a Ready Pod becomes Available and excludes terminating Pods from `availableReplicas`. With `maxUnavailable=0`, a 600-second availability delay therefore supplies a 60-second buffer over the 540-second Pod termination bound before another old Pod may be removed; the plan still requires a real-cluster assertion because terminating Pods can temporarily exceed `replicas + maxSurge` and controller/version behavior is part of the deployment contract.
- gRPC creates `ServerStreamTracer` before running server interceptors. The viable association is a per-stream mutable holder created by the tracer factory, inserted by `filterContext()`, and later CAS-bound to the permit by the interceptor; an interceptor-created Context cannot be assumed visible to an already-created tracer.
- Public `ServerTransportFilter` can tag transport Attributes but does not expose an individual transport close handle. Late gRPC transports must have send calls rejected via `ServerCall` Attributes and be terminated by max-age or global server shutdown, unless a deliberate Netty-internal channel registry is added.
- Strict rollout must freeze both HPA directions. A concurrent HPA scale-up can create several new-revision Pods that become Available together and permit multiple old-Pod deletions even when `minReadySeconds` exceeds Pod grace.
- Bootstrap needs admin health capability independently from lifecycle enforcement: all Pods first expose compatibility-mode 8082 while leases/drain remain off, then the chart switches NLB/probes and enables lifecycle. Otherwise a global health-port switch makes old Pods unhealthy simultaneously.

## rocketmq-helm Inspection Notes

- `/Users/lossend/pro/rocketmq-helm` is a separate Helm repository with a `.codegraph/` index, chart templates, tests, environment values, and operational scripts.
- No repository-local `AGENTS.md` is present.
- The Helm worktree already contains untracked environment files (`in.yaml`, `prod-euc.yaml`, `prod-in.yaml`, `test-in.yaml`); they belong to the user and must remain untouched.
- The main chart renders Proxy from `templates/proxy.yaml` and an optional gRPC NLB from `templates/proxy-nlb.yaml`; a separate `proxy-sg-standalone/` chart has its own deployment and services.
- The main chart already defaults Proxy rolling update to `maxSurge: 1`, `maxUnavailable: 0`, sets `terminationGracePeriodSeconds: 120`, renders a PDB, and defines startup/readiness/liveness probes.
- The current main-chart `preStop` invokes `./mqshutdown proxy || true`; this is process shutdown, not a readiness-withdrawal plus send-drain handshake.
- Proxy startup is overridden in the Pod spec with `sh -c './mqproxy ...'`, so signal correctness depends on the shell command and launcher chain rather than the image entrypoint alone.
- Main-chart Proxy startup is `/bin/sh -ec './mqproxy -pc ...'` without shell `exec`; the Pod therefore adds another signal-forwarding boundary before the already non-exec RocketMQ launcher chain.
- All three Proxy probes are TCP checks on the gRPC port. They prove only that the listener accepts connections: readiness cannot represent WARMING/DRAINING, and liveness cannot distinguish a healthy listener from a wedged request path.
- Main-chart PreStop immediately runs `./mqshutdown proxy || true`; `|| true` hides drain/shutdown failure from kubelet, and there is no wait for endpoint/LB withdrawal, send in-flight zero, or transport quiescence.
- The main chart defaults to two Proxy replicas and a fixed PDB `minAvailable: 1`; it has no HPA template and no `minReadySeconds` in the Deployment.
- The internal Proxy Service correctly relies on default `sessionAffinity: None` and does not publish unready endpoints. Its metrics port is exposed on the same ClusterIP Service as business traffic, while the planned admin health/drain port does not yet exist.
- The optional LoadBalancer Service exposes only gRPC and delegates all cloud-specific target type, health check, connection-draining/deregistration, and external traffic policy behavior to free-form annotations/defaults.
- Config checksum annotations already trigger Proxy rollout for config changes, which is useful once rollout/drain semantics are corrected.
- The standalone chart is materially weaker than the main chart: it defaults to one replica, has no explicit rollout strategy, termination grace period, PreStop, startup/liveness probes, PDB, HPA, anti-affinity, topology spread, priority class, or admin port.
- Standalone readiness is only a TCP gRPC-port probe with a 15-second initial delay; startup also uses `/bin/sh -ec './mqproxy ...'` without `exec`.
- Standalone AWS NLB uses IP targets, internal scheme, cross-zone balancing, and a TCP health check directly on port 8081. It does not configure target-group deregistration delay/connection termination or a readiness-aware admin health check.
- Standalone defaults include `replicas: 1` and debug enabled; this chart cannot meet a strict no-send-disturbance rollout or single-Pod deletion SLO without production overrides and template changes.
- `upgrade-proxy-standalone.py` executes `helm upgrade --install` and adds `--wait --timeout` for apply, but not `--atomic`; it performs no preflight spare-capacity/client-capability check and no post-rollout drain/send-SLO verification.
- The production upgrade helper has the same `--wait --timeout` shape. Helm's readiness wait is only as meaningful as the chart's current TCP readiness probe, so it can report success before Proxy dependencies/warmup are truly ready.
- Existing Proxy service fence/restore tooling correctly reasons about EndpointSlice `ready`, `serving`, and `terminating` conditions, but it is recovery-specific and currently expects exactly one restored Proxy; it is not a normal rolling-drain controller.
- Production main-chart values already request three Proxy replicas (five in the India overlay), while testing variants use one or two. The strict design can build on production's replica count but must fail validation for one-replica environments.
- Production values set `proxy.pdb.minAvailable: 2`, but `templates/proxy.yaml` hardcodes `minAvailable: 1`; the documented production override is currently ignored. `proxy.pdb.minAvailable` must be wired into the template and schema/tests.
- Alibaba NLB overlays enable a 30-second connection-drain setting, but current PreStop shuts Proxy down before readiness-aware withdrawal and the NLB health check still targets TCP 8081. AWS overlays configure TCP health checks but no explicit target-group deregistration/connection-termination contract.
- Existing standalone chart tests only assert namespace rendering. They do not assert lifecycle hooks, signal-safe command, probes, rollout strategy, grace period, PDB, scheduling, or NLB drain annotations.
- Main-chart tests assert the Proxy rolling strategy, but the current search found no chart-level lifecycle/drain/probe/PDB-value tests.
- Main-chart Proxy scheduling has required hostname anti-affinity but deliberately omits zone topology spread, and a regression test locks that behavior. Pods are separated across nodes, not guaranteed across zones; strict multi-zone availability needs a configurable Proxy zone-spread policy and surge scheduling headroom.
- The existing ServiceMonitor already scrapes the Proxy metrics port, so new lifecycle/drain metrics can reuse it. Alert rules and rollout gates still need to consume the metrics.
- If an admin health/drain port is added, it should be a container-only port (not the business or NLB Service). The chart's optional NetworkPolicy must be reviewed so kubelet health probes remain reachable while remote drain calls stay blocked; PreStop loopback remains local.
- Main-chart production values set an empty `proxy.nodeSelector: {}`. Because the helper switches entirely to Proxy-specific placement whenever that key exists, this suppresses inherited global nodeSelector/tolerations rather than merely adding no override.
- The optional NetworkPolicy currently selects `app.kubernetes.io/component=networkpolicy`, while Proxy Pods use `component=proxy`; it does not protect Proxy. Correcting it must account for client/NLB sources and the health/admin port.
- Production disables `metricCollectorMode`, although ServiceMonitor exists. Server-side lifecycle/in-flight/forced-drain metrics must remain locally exported regardless of client metric collection, or rollout gates will have no evidence.
- Main-chart Proxy config does not set a stable `remotingAccessAddr`; provider values must explicitly advertise the stable Remoting Service/LB before strict reconnect semantics can be claimed.
- Helm config validation is absent for several lifecycle-critical values. The standalone chart also accepts blank required `namesrvAddr`/`clusterName`, allowing TCP readiness to pass despite invalid dependencies; add `values.schema.json` and template cross-field checks.
- Fixed rollout scripts and chart defaults must use the same time-budget invariant: `terminationGracePeriod >= max(proxy drain path, provider deregistration path) + JVM shutdown + safety margin`. A provider delay longer than Pod grace only postpones target cleanup after the process is already gone.
