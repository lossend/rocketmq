# Findings

## Key Codebase Facts (verified against source)

- `ProxyConfig.java`: `proxy/src/main/java/org/apache/rocketmq/proxy/config/ProxyConfig.java` — plain fields + getter/setter, ~54.7K
- `ReceiptHandle.encode()` does NOT include group name — ack/changeInvisible re-read group from client request each time
- Consumer-side gRPC entry points reading `request.getGroup().getName()`:
  - `ReceiveMessageActivity.java:105`
  - `AckMessageActivity.java:56`
  - `ChangeInvisibleDurationActivity.java:51`
  - `ForwardMessageToDeadLetterQueueActivity.java` (same pattern)
- `AdminService` currently exposes only topic creation methods
- `MQClientAPIImpl.createSubscriptionGroup(addr, config, timeout)` exists at line 431
- `MQClientAPIExt` extends `MQClientAPIImpl` — reachable via `MQClientAPIFactory.getClient()`
- `ProxyContext.withVal(key, val)` / `getVal(key)` — generic value map
- Label transport: gRPC metadata header `__RMQ_TRAFFIC_LABEL` on ProxyContext
- G%label naming: `G` is origin group, `%` is separator, `gray1` is label → `G%gray1`

## SQL92 Merge Rule
- Gray: `__RMQ_TRAFFIC_LABEL = 'gray1'`
- Standard: `__RMQ_TRAFFIC_LABEL IS NULL OR __RMQ_TRAFFIC_LABEL = 'STANDARD'`
- With origin expression: `( a > 1 ) AND ( __RMQ_TRAFFIC_LABEL = 'gray1' )`
- Must AND-merge, never overwrite consumer's existing expression

## ProxyConfig Toggles (all default false)
- `enableTrafficLabelRouting = false` — master on/off
- `enableTrafficLabelGroupCleanup = false` — periodic cleanup of G%label groups
- `enableTrafficLabelRoutingLog = false` — routing-related logs
- `trafficLabelGroupCleanupIdleThresholdMs = 3600_000L` — 1 hour default
