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

## 2026-07-13 — Gray registration bootstrap

- `ClientActivity.registerConsumer` rewrites a gray group before `ClientProcessor.registerConsumer` validates it.
- `ClientProcessor.validateLiteMode` queries the broker subscription-group configuration before the consumer is registered.
- Broker configuration in sg-testing has `autoCreateSubscriptionGroup=false`; a new virtual group therefore fails this pre-registration query.
- The original topic-based bootstrap is insufficient: the source group can exist on only a subset of Master brokers and can have different configuration on each.
- The replacement must discover the configured cluster's Master brokers, inspect each broker's subscription-group table, and create the gray group only on the brokers that contain the original group.
- `MQClientAPIImpl.getAllSubscriptionGroup(brokerAddr, timeout)` supplies the per-broker table without treating a normal absence as `CODE: 26`; use it to determine both source and gray-group existence.
- A JSON round-trip through `RemotingSerializable` gives `SubscriptionGroupConfig` a deep copy, including retry policy, attributes, and subscription data.

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
