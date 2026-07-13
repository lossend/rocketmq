# Traffic Label Routing Plan-C Implementation

## Goal
Route POP consumption by traffic label entirely in the Proxy — zero broker code change. Gray consumers consume from G%label with SQL92 filter; standard consumers consume from G with complement filter. Master switch defaults OFF.

## Architecture
Three components: LabelRoutingResolver (stateless), TrafficLabelRouter (single facade, 4 activity entry points), LabelGroupBootstrapper (lazy create + dedup). Optional LabelGroupCleaner for periodic cleanup. All gated by ProxyConfig.enableTrafficLabelRouting=false.

## Plan Source
`docs/plans/2026-06-30-traffic-label-routing-plan-c-impl.md`

## Phases

| # | Task | Status | Owner |
|---|------|--------|-------|
| 1 | ProxyConfig toggles | complete | |
| 2 | TrafficLabel constants + effectiveGroup | complete | |
| 3 | LabelRoutingResolver (group + SQL92 merge) | complete | |
| 4 | TrafficLabelExtractor (read label from ProxyContext) | complete | |
| 5 | AdminService.createSubscriptionGroup / deleteSubscriptionGroup | complete | |
| 6 | LabelGroupBootstrapper (lazy create + dedup) | complete | |
| 7 | TrafficLabelRouter facade (master switch gating) | complete | |
| 8 | Wire router into 4 consumer-side activities | complete | |
| 9 | LabelGroupCleaner (optional periodic cleanup) | complete | |
| 10 | GrpcServerInterceptor: inject label from gRPC metadata into ProxyContext | complete | |
| 11 | E2E integration test | complete | |

## Tasks 1-7 are independent-ish (build up components bottom-up)
## Tasks 8-11 depend on 1-7 being complete

## Task 12 (Gray Group Bootstrap Hardening — Bug Fix)

| # | Task | Status | Owner |
|---|------|--------|-------|
| 12a | DefaultAdminService.cloneSubscriptionGroupIfAbsent → cluster-wide (source once, create on ALL masters) | complete | agent-admin |
| 12b | MetadataService.invalidateSubscriptionGroupConfig + ClusterMetadataService/LocalMetadataService impls | complete | agent-meta |
| 12c | LabelGroupBootstrapper holds MetadataService; invalidates cache after successful ensure | complete | agent-meta |
| 12d | TrafficLabelRouter.resolveGray safety-net (cache-gated ensure on receive path) | complete | agent-meta |
| 12e | DefaultGrpcMessagingActivity wiring (pass MetadataService to bootstrapper and router) | complete | agent-meta |
| 12f | Tests: DefaultAdminServiceTrafficLabelTest updated for cluster-wide semantics | complete | agent-admin |
| 12g | Tests: LabelGroupBootstrapperTest + TrafficLabelRouterTest extended for cache invalidation and safety net | complete | agent-meta |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
