# Task Plan: RocketMQ Proxy Graceful Lifecycle

## Goal

Produce and persist a decision-complete, server-managed design for graceful Proxy scale-out and restart so supported Java gRPC and Remoting producers see zero observed send errors/timeouts and bounded tail-latency growth behind a Kubernetes Service/load balancer, without changing Java SDK code.

## Decisions

- Change only Proxy Server and `/Users/lossend/pro/rocketmq-helm`; do not modify Java SDKs and do not add a Gateway.
- Cover `rocketmq-client-java` 5.0.7 and 5.2.1 strictly, plus Remoting client 5.3.2+; treat Remoting 5.2.0 and older as degraded compatibility.
- Target stable Kubernetes Service/load-balancer access, not direct Pod addressing.
- Use five-minute randomized connection leases, server-driven GO_AWAY, lifecycle-aware readiness, exact send drain, and a 540-second Pod termination budget.
- Require zero observed acknowledged-send errors/timeouts, event-window p99 <= baseline +100 ms, and p999 <= baseline +500 ms.
- Exclude `sendOneway`, SIGKILL, node loss, network partitions, and concurrent multi-Pod deletion from the strict guarantee.
- Preserve RocketMQ at-least-once semantics; duplicate messages are recorded but are not an acceptance failure.
- Apply the strict deployment contract to the main chart; keep the single-replica standalone chart explicitly degraded.

## Current Phase

Complete

## Phases

### Phase 1: Inspect Current Behavior

- [x] Trace Proxy startup/shutdown ordering, gRPC shutdown, Remoting GO_AWAY, send execution, and Java client retry behavior.
- [x] Check deployment assets and upstream RocketMQ changes.
- **Status:** complete

### Phase 2: Design Coordinated Lifecycle

- [x] Define lifecycle states, readiness behavior, two-phase drain, in-flight accounting, client preconnection, and rollout constraints.
- [x] Compare alternatives and select the recommended architecture.
- **Status:** complete

### Phase 3: Persist and Hand Off

- [x] Save the implementation plan under `docs/plans/`.
- [x] Self-review assumptions, compatibility, failure modes, observability, and acceptance tests.
- [x] Deliver the concise design answer to the user.
- **Status:** complete

### Phase 4: Document Current-State Problems

- [x] Revalidate the current Proxy lifecycle gaps against the indexed source.
- [x] Add a prioritized current-state problem inventory with code behavior and send impact.
- [x] Verify the updated canonical plan and planning records.
- **Status:** complete

### Phase 5: Align with rocketmq-helm

- [x] Inspect the actual Proxy workload, Service, probes, lifecycle hooks, rollout, autoscaling, disruption, and values in `/Users/lossend/pro/rocketmq-helm`.
- [x] Map chart defaults and gaps to the Proxy lifecycle design without changing chart runtime code.
- [x] Add concrete Helm template/values changes, migration order, and chart-level acceptance checks to the canonical plan.
- [x] Verify the updated plan and planning records.
- **Status:** complete

### Phase 6: Replace SDK-Dependent Design

- [x] Analyze `rocketmq-client-java` 5.2.1 in addition to 5.0.7 and separate it from the nonexistent classic Remoting 5.2.1 artifact.
- [x] Remove SDK dual-hot channels, client Drain ACK, new protocol messages, and the 120-second shutdown assumption.
- [x] Define the server-managed connection lease, lifecycle state machine, exact drain, Helm timing contract, compatibility matrix, and bootstrap boundary.
- [x] Preserve the original SDK-dependent plan and save the Server/Helm-only replacement as a separate Markdown plan.
- **Status:** complete

### Phase 7: Harden the Server-Managed Plan

- [x] Define the old plan as non-normative Proxy implementation reference and make the new plan authoritative.
- [x] Move Remoting admission before executor submission and specify CAS-linearized send admission plus backend/protocol dual terminals.
- [x] Define runtime ownership, initial warmup/readiness, immutable drain cutoffs, configuration defaults, and transport termination boundaries.
- [x] Correct rollout serialization, HPA coordination, health-port security, first-bootstrap compatibility, and controlled rollback semantics.
- [x] Expand unit, provider, client-version, failure, and mixed-version acceptance coverage and add an executable implementation sequence.
- [x] Verify document consistency, preserve the original plan, and update planning records.
- **Status:** complete

## Errors Encountered

| Error | Attempt | Resolution |
|---|---:|---|
| Shell glob `Dockerfile*` had no match | 1 | Switched to quoted/literal `rg` file discovery. |
| One-way send clarification was interrupted | 1 | Adopted the explicit default that `sendOneway` is outside the strict guarantee. |
| `rtk test -s` was not proxied as the shell builtin | 1 | Verify non-empty files with `rtk wc -c` instead. |
| A stale-text regex used invalid escaped full-width parentheses | 1 | Switched the check to fixed-string `rg -F` patterns. |
| A read-only reviewer created an unrequested duplicate design document | 1 | Removed the duplicate and retained only the reviewed canonical plan under `docs/plans/`. |
