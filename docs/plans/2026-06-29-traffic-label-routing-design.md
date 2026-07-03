---
name: traffic-label-routing-design
description: RocketMQ dynamic consumption routing based on traffic labels - top-level design and plan index
date: 2026-06-29
status: brainstorming
---

# Dynamic Consumption Routing Based on Traffic Labels

> This document is the top-level design index. The two candidate plans are:
> - [[2026-06-29-traffic-label-routing-plan-a-strict-bypass]] - Plan A: strict isolation plus bypass storage
> - [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] - Plan B: per-label sub-cursors plus standard harvesting
>
> Historical reference only. This design was reconsidered independently and is not bound to reuse it:
> - [[2026-06-26-traffic-label-routing-server-side-pop-retry]] - the old route-topic / POP retry plan

## 1. Background and Requirements

The deployment contains two kinds of environments:

- **Standard environment**: long-lived and responsible for the full fallback traffic set.
- **Isolated environments**: one or more can exist at the same time, such as gray1, gray2, and so on, used for gray release, PR preview, load testing, and similar scenarios.

An application may be deployed either in the standard environment or in one of the isolated environments. The expected behavior is:

1. Producers in an isolated environment attach an **isolation tag**, meaning a traffic label, to every message, using the `__RMQ_TRAFFIC_LABEL` message property.
2. If a consumer in an isolated environment with the **same traffic label is online**, that message is consumed by the corresponding isolated environment.
3. Otherwise, the message **falls back** to consumers in the standard environment.

Constraints: **the Proxy is responsible for consumption routing**, and the solution must support **distributed deployment**, **high availability**, and **high performance**.

Consumption model: **POP / gRPC 5.x only**. This has been confirmed. There is no classic PULL or PUSH model involved, which makes the situation cleaner.

## 2. Core Tension: POP Uses a Single Cursor

Under POP, one `(consumerGroup, topic, queueId)` shares **one offset cursor**. This requirement wants to decide, at **single-message granularity** on the same physical queue, who should consume each message. That collides directly with the single-cursor model.

> "Sequential single cursor" + "selectively skip some messages" + "messages skipped today must still be consumable later by some other environment" cannot all be satisfied cheaply at the same time.

Skipping a message is **one-way**. Once the cursor moves past it, the message has effectively been handed to the other side, and the skipper will not come back to it. Therefore, if a message skipped by standard must still be able to fall back to standard later, **plain SKIP is not enough**. There are only two real choices:

- **A. Copy the message into a bypass path**. Before skipping it, persist it elsewhere so the isolated consumer can read it there.
- **B. Do not copy it. Instead let standard consumers harvest the sub-cursor of an offline isolated label**. Give each traffic label its own independent sub-cursor, and let standard consumers drain that sub-cursor once the label is offline.

Those are exactly Plan A and Plan B.

## 3. Architectural Conclusions

### 3.1 Can This Be Done Using Extension Points Only?

**Not fully.** According to [[Server_Extension_Points]](`docs/cn/Server_Extension_Points.md`):

- Proxy-side `PopMessageResultFilter.FilterResult` only supports `{TO_DLQ, NO_MATCH, MATCH, TO_RETURN}`. It cannot rewrite headers, move a message across topics, or trigger fallback pulling.
- Store-side `MessageFilter`, through `isMatchedByConsumeQueue` and `isMatchedByCommitLog`, does get invoked on the POP path and can decide match vs skip based on message properties. However, it **has no online-view state and cannot perform stateful fallback routing**.

Conclusion: pure extension points can at best implement a **weakened version** consisting of exact isolation matching plus blind standard fallback. That does not satisfy the dynamic semantics of "isolate when online, fall back when offline."

> Clarification added on 2026-06-29: after decomposing plan-b into five required capabilities and matching them against extension points one by one, it turns out there is a **zero-broker-intrusion static isolation variant, Plan-B-Lite**, built from pre-created real groups `G%label`, SQL92 property filtering, and `enablePropertyFilter`. However, it still **lacks dynamic fallback**. The full comparison table and the two fallback paths are documented in [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] sections 8 and 9. The key findings are:
> - Proxy `PopMessageResultFilter.NO_MATCH` will **ack and lose the message**, so it is a fallback trap and cannot be used.
> - Dynamic fallback requires 1) an online-label snapshot and 5) harvesting fan-in. **No extension point maps to either of those**, so core changes or an external operator are unavoidable.

### 3.2 Both Proxy and Broker Must Change

Dynamic fallback depends on a **traffic-label online snapshot**, which is a cluster-wide stateful view of which labels exist and whether they are online. That implies:

- **Proxy side**: maintain a cross-instance online-label snapshot through `ClusterConsumerManager` plus `HeartbeatSyncer`, and send `consumerLabel` together with the online snapshot down to the broker with POP requests.
- **Broker side**: centralize per-message routing decisions in `PopMessageProcessor`.

Both candidate plans share this Proxy-plus-Broker baseline. Their difference lies only in how the broker handles isolated messages that standard consumers skip.

### 3.3 Source-Level Facts That Affect the Plan Choice

Comparing the design against source code locked in the following facts:

| Fact | Anchor | Impact |
|---|---|---|
| Offset accounting key is `topic@group` | `ConsumerOffsetManager.java:201,241` | Plan B should implement sub-cursors through **virtual subscription groups `G%label`**, reusing existing accounting without a new offset table |
| **The POP path does not support LMQ**. `PopMessageProcessor` has zero `isLmq` references, and LMQ is only used for PULL | `PopMessageProcessor.java`, `LmqPullRequestHoldService` | **The LMQ candidate is ruled out**. See [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] section 0 |
| Per-message deliver-or-skip decisions happen in `messageFilter`, while cursor advancement remains unchanged | `PopMessageProcessor.java:774-776` | Both plans can hang label-based decisions on the filter without touching cursor advancement |
| A virtual group is rejected by `findSubscriptionGroupConfig` | `PopMessageProcessor.java:308` | Plan B must modify this point so virtual groups inherit parent-group config |

## 4. Side-by-Side Comparison

| Dimension | Plan A: strict isolation plus bypass storage | Plan B: sub-cursors plus standard harvesting |
|---|---|---|
| Core mechanism | Copy isolated messages into bypass storage before skipping them, then let isolated consumers read from bypass | Give each traffic label its own sub-cursor, and let standard consumers harvest it when the label is offline |
| Whether the message body is copied | Yes | No |
| Whether route topics or bypass storage are needed | Yes | No |
| Number of offset records | Standard plus bypass | label count x queue count, growing with label count |
| Revive / retry | Needs special handling for bypass | Reuses each sub-cursor's POP retry directly, with zero special handling |
| Fallback timeliness | High, because bypass delivers independently | Medium, because it depends on standard harvesting rounds |
| Main risk | Copy amplification, bypass consistency, and complex revive special handling | Sub-cursor metadata growth, harvesting races, and degradation when label count is high |
| Complexity | High | Medium |
| Sub-cursor carrier | route topic, like the old plan | **Virtual subscription group `G%label`**, reusing `topic@group` accounting. LMQ is no longer considered |

## 5. Shared Problems Both Plans Must Handle

- **Cross-environment leakage during state flaps**. The online/offline judgment for isolated labels is snapshot-based and has propagation delay.
- **At-least-once duplicates**. Residual windows in harvesting or bypass switching require idempotent consumers, which RocketMQ already assumes.
- **Accuracy and propagation delay of the online-label snapshot** are central to the availability of both plans.
- **Retry topic plus revive asynchronous orphan risk**. Failed messages in isolated environments go to a virtual-group-specific retry topic like `%RETRY%G%gray1`, and revive resubmits asynchronously. Before deleting a virtual group, the retry topic must be drained and revive must have no leftovers, or orphan messages remain. This is specific to plan B.
- **Broker master/slave switching and restart**. Harvest queues and grace-period timers are in-memory derived state. They should follow RocketMQ's existing pattern: zero persistence, hook into `changeSpecialServiceStatus`, and rebuild cold, without losing messages. This is also specific to plan B.
- **Same-name rebuild semantics in temporary environments**. The user chose "a rebuilt gray environment should take over its predecessor's backlog." That requires a shared persistent cursor `G%label` without epoch in plan B, with grace period as the single knob between "wait for gray to come back" and "fallback to standard."
  - Warning: the tension is fundamental. A message cannot both wait for gray to return and also be given immediately to standard. Under takeover semantics, **fallback is delayed by exactly the grace period**.

## 6. Chosen Direction (Locked on 2026-06-29)

**Locked premise from the user**: isolated environments are **temporary**, such as PR preview or load-test environments that are created and destroyed frequently. When gray goes offline, messages should **prefer waiting for a same-name gray rebuild to take over**, and only fall back to standard if gray does not come back within the grace period.

Therefore:

- **Plan-B-Lite static mode is ruled out**. A destroyed gray environment will never return, so pure virtual groups with no harvesting would leave backlog forever unconsumed.
- **Full plan-b harvesting mode is selected**. See [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] sections 4, 4a, and 4b.
- Temporary environments force three hard points:
  1. **Grace period `gracePeriodMs`** is the unified knob between takeover and fallback. If gray returns within the grace period, it takes over the full backlog from the persistent cursor. Only after timeout does standard harvesting begin.
  2. **Persistent shared cursor without epoch**. `G%grayX` survives across generations, and a same-name gray rebuild resumes from the existing cursor.
  3. **Virtual-group cleanup** after harvesting completes, so that create-destroy cycles do not bloat metadata forever. Cleanup must use the full three-condition check, and it must re-check the online snapshot in a cleanup critical section to avoid deleting the cursor of a returning owner.
- Harvesting should be driven by **broker-side fan-in with round-robin piggybacking**, keeping the normal path at zero amplification and the harvesting path bounded to 1+1.

> Plan A, [[2026-06-29-traffic-label-routing-plan-a-strict-bypass]], remains as a comparison option only if label count grows so large that virtual-group metadata becomes too expensive.

### Next Steps

- [x] Converge plan-b into an implementable design. Component boundaries in section 10.1 and data flow in section 10.2 are already finished in [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] section 10
- [x] Add E2E and API test case design per planning rules. The five E2E cases plus the API contract table are already in [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] section 11
- [ ] Turn design into an implementation plan and then implement it
