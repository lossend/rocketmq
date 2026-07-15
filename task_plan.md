# Task Plan: RocketMQ Proxy Graceful Lifecycle

## Goal

Produce a decision-complete design for graceful Proxy scale-out and restart so Java gRPC and Remoting producers see nearly zero additional send failures or tail-latency spikes behind a Kubernetes Service/load balancer.

## Decisions

- Cover both gRPC and Remoting protocol entries.
- Target Kubernetes Service/load-balancer access, not direct Pod addressing.
- Coordinate Proxy, Kubernetes deployment, and both Java client implementations.
- Optimize for near-zero additional send failures and p99/p999 disturbance during planned scale/restart.
- Exclude `sendOneway` from the zero-failure guarantee because it has no acknowledgement semantics.
- Preserve compatibility through additive configuration and capability-aware fallback.

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

## Errors Encountered

| Error | Attempt | Resolution |
|---|---:|---|
| Shell glob `Dockerfile*` had no match | 1 | Switched to quoted/literal `rg` file discovery. |
| One-way send clarification was interrupted | 1 | Adopted the explicit default that `sendOneway` is outside the strict guarantee. |
| `rtk test -s` was not proxied as the shell builtin | 1 | Verify non-empty files with `rtk wc -c` instead. |
| A stale-text regex used invalid escaped full-width parentheses | 1 | Switched the check to fixed-string `rg -F` patterns. |
| A read-only reviewer created an unrequested duplicate design document | 1 | Removed the duplicate and retained only the reviewed canonical plan under `docs/plans/`. |
