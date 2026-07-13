# Progress Log

## Session 1 — 2026-06-30

### Session Start
- Worktree: /Users/lossend/opensource/rocketmq-5.5.0 (branch: traffic-label-5.5.0, tag: rocketmq-all-5.5.0)
- Plan: docs/plans/2026-06-30-traffic-label-routing-plan-c-impl.md
- Starting implementation with teammate orchestration (11 tasks)

### Work Log

## Session 2 — 2026-07-13

### Session Start
- Branch: traffic-label-5.5.0 → mayfair/traffic-label-5.5.0
- Task: Fix "No group in this broker … group: g-push-test%syj-dev" in sg-testing

### Root cause diagnosed
1. `cloneSubscriptionGroupIfAbsent` only cloned onto masters hosting the origin group; random-broker lookup could hit any master → intermittent miss.
2. No cache invalidation: 300s negative cache masked a freshly-created group, causing repeated log noise every ~20s.
3. No consume-path safety net: transient provisioning failure left gray consumers permanently broken until restart.

### Changes (commit 89d4afe0d)
- DefaultAdminService.cloneSubscriptionGroupIfAbsent → two-pass: source config from first master that has origin, create on ALL cluster masters.
- MetadataService.invalidateSubscriptionGroupConfig added; ClusterMetadataService implements (cache.invalidate); LocalMetadataService no-op.
- LabelGroupBootstrapper: holds MetadataService; invalidates after successful clone.
- TrafficLabelRouter.resolveGray: cache-gated safety net re-provisions if group absent from metadata cache.
- DefaultGrpcMessagingActivity: passes MetadataService to bootstrapper and router.

### Test results
- 29 traffic-label tests pass (TrafficLabelRouterTest 13, ClientActivityTrafficLabelTest 6, LabelGroupBootstrapperTest 4, DefaultAdminServiceTrafficLabelTest 6).
- Full proxy suite blocked by pre-existing JDK21 + JaCoCo 0.8.5 + SpotBugs incompatibility (unrelated to these changes).

### Status: Task 12 COMPLETE — pushed to remote

### Code review fixes (commit 0ae327e38)
- H1: 30s per-group backoff in TrafficLabelRouter.resolveGray — prevents cluster RPC flood on every receive when origin group is missing.
- M3/M4: Updated AdminService and TrafficLabelRouter Javadoc to match actual cluster-wide behavior.
- M5: Collapsed two-pass table fetch in cloneSubscriptionGroupIfAbsent into single pass (halved broker RPCs).
- L6: Extracted BROKER_RPC_TIMEOUT_MS constant in DefaultAdminService.
- M2 (partial success on flaky broker): pushed back — intentional fail-fast design.
- L7/L8: pre-existing low-ROI items, skipped.
