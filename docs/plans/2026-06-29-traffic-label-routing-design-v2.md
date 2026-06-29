# Traffic Label Routing Design

**Date**: 2026-06-29  
**Author**: yangjie.sun  
**Status**: Draft

---

## Background

RocketMQ deployments often include a stable **standard** environment alongside temporary **isolated** environments (e.g., gray1, gray2) used for canary releases, A/B testing, or traffic isolation. The goal is to route messages carrying a traffic label to the matching isolated consumer group while falling back gracefully when the target consumers are offline.

---

## Goals

1. Producer attaches `__RMQ_TRAFFIC_LABEL` property to messages destined for an isolated environment.
2. Isolated consumers only consume messages whose label matches their own.
3. Standard consumers only consume unlabeled messages, falling back to consume labeled messages when no matching isolated consumer is online.
4. Isolated consumers fall back to consuming unlabeled (standard) messages when no standard consumer is online.
5. Fallback is **immediate** — no grace period.
6. Consumers have **zero awareness** of routing logic; the Proxy handles everything.
7. Minimal source code changes to the RocketMQ codebase.

---

## Non-Goals

- Does not support PULL/PUSH model (remoting 4.x). Only the gRPC 5.x POP model is in scope.
- Does not support per-queue or per-partition label affinity.
- Does not change message storage or broker internals.

---

## Architecture Overview

```
Producer
  └─ sets msg.putUserProperty("__RMQ_TRAFFIC_LABEL", "gray1")

Proxy (ReceiveMessageActivity)
  └─ popMessage(..., TrafficLabelPopFilter)
       └─ TrafficLabelPopFilter.filterMessage(ctx, group, sub, msg)
            ├─ ctx.getClientID()           → consumer's label
            ├─ msg.__RMQ_TRAFFIC_LABEL     → message's label
            ├─ ConsumerGroupInfo.getAllClientId() → online label set
            └─ returns MATCH | TO_RETURN

Broker
  └─ TO_RETURN → changeInvisibleTime (message becomes re-consumable)
  └─ MATCH     → message delivered to consumer
```

---

## Key Design Decisions

### Label Carrier: clientId (gRPC metadata)

The gRPC 5.x SDK sends a `x-mq-client-id` header with every request. `ContextInitPipeline` reads it into `ProxyContext.clientId`. The filter accesses it via `ctx.getClientID()`.

Consumers embed their traffic label in the clientId using the format:

```
{label}@{ip}@{instanceName}
```

- Standard consumer: `@192.168.1.1@default` (label is empty string before `@`)
- Isolated consumer: `gray1@192.168.1.2@default`

Label parsing:

```java
private static String parseLabel(String clientId) {
    if (clientId == null) return "";
    int idx = clientId.indexOf('@');
    return idx > 0 ? clientId.substring(0, idx) : "";
}
```

### Routing Logic

The routing rule is symmetric: both sides fall back to consuming the other's messages when the target is offline.

| Consumer label | Message label | Target online? | Result |
|---------------|---------------|----------------|--------|
| gray1 | gray1 | — | MATCH |
| "" | "" | — | MATCH |
| gray1 | "" | standard online | TO_RETURN |
| gray1 | "" | standard **offline** | MATCH (fallback) |
| "" | gray1 | gray1 online | TO_RETURN |
| "" | gray1 | gray1 **offline** | MATCH (fallback) |

### Online Detection

`ConsumerGroupInfo.getAllClientId()` returns all clientIds currently registered under a consumerGroup. These represent active connections — disconnected clients are evicted by `ClientHousekeepingService`. No topic-level indexing is needed because all clients under the same consumerGroup subscribe to the same topic set.

```java
boolean targetOnline = groupInfo.getAllClientId().stream()
    .anyMatch(id -> targetLabel.equals(parseLabel(id)));
```

### TO_RETURN vs NO_MATCH

`NO_MATCH` calls `ackMessage()` — the message is permanently consumed. `TO_RETURN` calls `changeInvisibleTime()` — the message becomes invisible temporarily and re-enters the queue for re-delivery. Only `TO_RETURN` is safe for routing.

---

## Implementation

### New class: `TrafficLabelPopFilter`

**File**: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/TrafficLabelPopFilter.java`

```java
public class TrafficLabelPopFilter implements PopMessageResultFilter {

    private static final String TRAFFIC_LABEL_PROP = "__RMQ_TRAFFIC_LABEL";

    private final PopMessageResultFilter delegate;
    private final MessagingProcessor messagingProcessor;

    public TrafficLabelPopFilter(PopMessageResultFilter delegate,
                                 MessagingProcessor messagingProcessor) {
        this.delegate = delegate;
        this.messagingProcessor = messagingProcessor;
    }

    @Override
    public FilterResult filterMessage(ProxyContext ctx, String consumerGroup,
            SubscriptionData subscriptionData, MessageExt messageExt) {

        FilterResult base = delegate.filterMessage(ctx, consumerGroup, subscriptionData, messageExt);
        if (base == FilterResult.TO_DLQ) {
            return FilterResult.TO_DLQ;
        }

        String msgLabel = messageExt.getUserProperty(TRAFFIC_LABEL_PROP);
        if (msgLabel == null) msgLabel = "";
        String consumerLabel = parseLabel(ctx.getClientID());

        if (msgLabel.equals(consumerLabel)) {
            return base;
        }

        String targetLabel = msgLabel;
        ConsumerGroupInfo groupInfo = messagingProcessor.getConsumerGroupInfo(ctx, consumerGroup);
        boolean targetOnline = groupInfo != null && groupInfo.getAllClientId().stream()
                .anyMatch(id -> targetLabel.equals(parseLabel(id)));

        return targetOnline ? FilterResult.TO_RETURN : base;
    }

    private static String parseLabel(String clientId) {
        if (clientId == null || clientId.isEmpty()) return "";
        int idx = clientId.indexOf('@');
        return idx > 0 ? clientId.substring(0, idx) : "";
    }
}
```

### Modified: `ReceiveMessageActivity`

**File**: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/ReceiveMessageActivity.java`

Two places, both replace `new PopMessageResultFilterImpl(maxAttempts)`:

**Line 155** (lite consumer path — `popLiteMessage`):
```java
// Before
new PopMessageResultFilterImpl(maxAttempts),

// After
new TrafficLabelPopFilter(new PopMessageResultFilterImpl(maxAttempts), messagingProcessor),
```

**Line 173** (normal consumer path — `popMessage`):
```java
// Before
new PopMessageResultFilterImpl(maxAttempts),

// After
new TrafficLabelPopFilter(new PopMessageResultFilterImpl(maxAttempts), messagingProcessor),
```

---

## Change Summary

| File | Change type | Description |
|------|-------------|-------------|
| `TrafficLabelPopFilter.java` | New file | Core routing logic |
| `ReceiveMessageActivity.java` | 2 lines modified | Wrap existing filter (lite + normal paths) |

No broker changes. No protocol changes. No SDK changes to RocketMQ server.

---

## Client Configuration

gRPC 5.x SDK consumers set clientId via `ClientConfiguration`:

```java
// Standard consumer
ClientConfiguration config = ClientConfiguration.newBuilder()
    .setEndpoints("...")
    // clientId defaults to auto-generated; prefix with "@" or leave as-is
    .build();

// Isolated consumer (gray1)
ClientConfiguration config = ClientConfiguration.newBuilder()
    .setEndpoints("...")
    .setClientId("gray1@" + InetAddress.getLocalHost().getHostAddress() + "@myApp")
    .build();
```

---

## Edge Cases

| Scenario | Behavior |
|----------|----------|
| Standard consumer offline, labeled message arrives | Isolated consumer consumes (fallback MATCH) |
| Isolated consumer offline, labeled message arrives | Standard consumer consumes (fallback MATCH) |
| Both standard and isolated online | Each consumes only their own labeled messages |
| Message has no `__RMQ_TRAFFIC_LABEL` | Standard consumer consumes; isolated consumer returns it if standard is online, else fallback |
| Multiple isolated environments (gray1, gray2) | Each env only consumes its own label; falls back independently |

---

## Limitations

1. **Online detection granularity**: `getAllClientId()` is group-level, not topic-level. Acceptable because same-group consumers subscribe to the same topics.
2. **Latency on TO_RETURN**: Messages returned to invisible state wait for `invisibleTime` to expire before re-delivery. For immediate routing, `invisibleTime` should be set low (e.g., 1–2s).
3. **gRPC SDK clientId customization**: The SDK must expose `setClientId()`. Verify against the specific SDK version in use.
4. **Cluster mode**: In cluster-mode Proxy, `ConsumerGroupInfo` is local to each Proxy node. A consumer connected to Proxy-A is not visible to Proxy-B. If needed, a distributed label registry (e.g., Redis) can replace `getAllClientId()` — this is a future extension point.

---

## Test Cases

### Unit Tests

1. `filterMessage` returns MATCH when msgLabel equals consumerLabel.
2. `filterMessage` returns TO_RETURN when msgLabel differs and target consumer is online.
3. `filterMessage` returns MATCH (fallback) when msgLabel differs and target consumer is offline.
4. `filterMessage` returns TO_DLQ when delegate returns TO_DLQ (max retries exceeded).
5. `parseLabel` correctly extracts label from `gray1@ip@app`, `@ip@app`, and `null`.

### Integration Tests

1. **Normal routing**: gray1 producer → gray1 consumer receives; standard consumer does not.
2. **Fallback on isolated offline**: gray1 labeled message, no gray1 consumer online → standard consumer receives.
3. **Fallback on standard offline**: unlabeled message, no standard consumer online → gray1 consumer receives.
4. **Multiple isolated envs**: gray1 and gray2 consumers online; gray1 messages go to gray1 only.
5. **Regression**: existing tag-filter and max-retry logic unaffected.
