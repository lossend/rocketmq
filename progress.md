# Progress: RocketMQ Proxy Graceful Lifecycle

## 2026-07-15

- Completed structural inspection of Proxy lifecycle, gRPC server/application shutdown, Remoting server shutdown, send entry points, and Java Remoting GO_AWAY behavior.
- Verified upstream motivation and compatibility behavior for GO_AWAY.
- Inspected the current Java gRPC client's channel and producer retry implementation.
- Locked scope with the user: both protocols, Kubernetes Service/LB, strict failure and tail-latency target, Proxy + Kubernetes + Java SDK changes.
- Began the coordinated lifecycle design; no production code has been changed.
- Confirmed Kubernetes endpoint-termination, PreStop ordering, gRPC probe, and rolling-update semantics from current official documentation.
- Completed an independent protocol and Kubernetes adversarial review; identified admission linearization, gRPC completion, distinct-backend, termination-ordering, LB-specific, and statistical-SLO corrections to apply.
- Verified the Docker/launcher signal path and confirmed that the current `mqproxy -> runserver -> java` chain lacks `exec` at the final two hops.
- Final verification found two check-command issues (not plan defects); recorded them and switched to portable file-size and fixed-string checks.
- Revised the plan to linearize admission, bind completion to backend and response terminal states, actively drain from PreStop, verify distinct Proxy instances, forward TERM to Java, and narrow strict SLO claims to a measured LB/client environment.
- Verification passed: formal plan is non-empty, all seven sections and required contracts are present, and no placeholders, stale design phrases, or trailing whitespace remain.
- Removed an unrequested duplicate document created during parallel review and aligned `task_plan.md` with the skill's canonical status/checklist format.
- Reopened the plan to add a prioritized inventory of the current Proxy graceful online/offline problems requested by the user.
- Revalidated the gRPC request/shutdown path in CodeGraph: send completion is asynchronous, telemetry is long-lived, server termination result is ignored, and application executors are not awaited.
- Added a P0/P1/P2 current-state problem inventory to the canonical plan, covering signal delivery, lifecycle/admission, both protocols, cold startup, long-connection rebalancing, Kubernetes coordination, resource deadlines, observability, and semantic boundaries.
- Reopened the plan to align the graceful lifecycle design with the concrete `/Users/lossend/pro/rocketmq-helm` deployment project; no Helm runtime files will be changed in this phase.
- Confirmed the Helm repository has no local `AGENTS.md` and recorded its pre-existing untracked environment files so this documentation-only review does not modify them.
- Located the main and standalone Proxy charts and identified the first concrete lifecycle facts: zero-unavailable rollout and 120-second grace already exist, while PreStop directly calls `mqshutdown` and the container command introduces a shell process.
- Read the complete main Proxy Deployment/Service/PDB and defaults: probes are TCP-only, replicas default to 2, PDB is fixed at minAvailable 1, no HPA/minReadySeconds/admin port exists, and NLB drain behavior is annotation-defined rather than chart-contracted.
- Read the standalone chart: it has one default replica, a 30-second implicit Kubernetes grace period, only TCP readiness, no PreStop/PDB/HPA/explicit rollout or scheduling resilience, and an AWS NLB whose health check targets the gRPC listener directly.
- Inspected the Helm upgrade tooling structurally: apply waits for Kubernetes readiness but lacks atomic rollback and graceful-drain/SLO gates; current TCP readiness therefore makes the wait weaker than the desired lifecycle contract.
- Checked tracked production/testing values and tests. Found a concrete configuration bug: production asks for Proxy PDB minAvailable 2, but the template hardcodes 1. Also confirmed provider NLB drain settings are inconsistent and lifecycle behavior lacks rendering tests.
- Reviewed scheduling, NetworkPolicy, and monitoring integration: Proxy is node-separated but not zone-spread, ServiceMonitor can carry the proposed metrics, and the future admin port must stay outside business Services while remaining probe-accessible.
- Verified current provider documentation: AWS exposes target-group deregistration delay/connection termination attributes absent from the chart, and Alibaba explicitly requires connection drain to cooperate with readiness and PreStop rather than replace application drain.
- Re-read the canonical Kubernetes/config/test sections and prepared a dedicated Helm alignment section so chart facts, required template changes, provider-specific drain settings, and rendering tests remain distinct from Proxy runtime implementation.
- Completed parallel read-only reviews of the main and standalone charts. Added findings for placement inheritance, ineffective NetworkPolicy selector, disabled production metric collection, missing stable Remoting address, schema gaps, and the shared Proxy/LB termination-budget invariant.
- Added a dedicated `rocketmq-helm` alignment section to the canonical plan: current main/standalone gap matrix, concrete template and values contract, AWS/ACK NLB coordination, standalone scope, upgrade gates, and chart/integration acceptance tests.
- Fresh verification passed: six targeted Helm chart tests, main-chart lint, standalone-chart lint, required document contracts, placeholder scan, and trailing-whitespace scan all completed with zero failures.
