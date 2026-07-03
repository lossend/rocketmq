---
name: traffic-label-routing-plan-c-proxy-only
description: Plan C - Proxy-only, no fallback: the consumer declares a label, the Proxy transparently rewrites the group name + SQL92 filter, admin lazily creates the real group G%label, and the broker requires zero code changes
date: 2026-06-30
status: brainstorming
verified: verified against source (ReceiptHandle / ReceiveMessageActivity / DefaultAdminService / MQClientAPIImpl)
---

# Plan C: Proxy-Only, No Fallback

> This document is a **simplified derivative** of [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]]:
> it **removes the "fallback to standard" capability**, switches to Proxy-side extension logic, and keeps the change set to **Proxy only, with zero broker code changes**.
> See [[2026-06-29-traffic-label-routing-design]] for the top-level design and plan index.
>
> Warning: this document does not modify the original plan-b. Plan-b remains the full version for cases that require dynamic fallback.

## 0. One-Sentence Positioning

> Each traffic label maps to one **real subscription group** `G%label`, with its own independent cursor, consuming only messages for that label.
> The Proxy translates the consumer's **declared label** into **group name + SQL92 filtering**, and lazily creates the real group through admin.
> **There is no fallback, so there is no online snapshot, harvesting, grace period, or cleanup. The entire mechanism set disappears together with fallback.**

## 1. What Was Removed Compared with Plan-B

Removing the requirement that "messages must fall back to standard after a gray environment goes offline" eliminates most of plan-b's complexity. The table below explains the **only reason each mechanism exists**, and why that mechanism loses its meaning once fallback is removed.

| Plan-b mechanism | The only reason it exists | This plan | Anchor (plan-b) |
|---|---|---|---|
| Online snapshot `LabelSnapshotManager` | To know which labels are offline and therefore need harvesting | Removed | plan-b section 10.1 |
| Harvesting `HarvestScheduler` + standard fan-in | To let standard consumers take over backlog from offline gray environments | Removed | plan-b section 4 decision A |
| Grace period `GracePeriodTimer` | Unified knob for "take over vs fallback" | Removed, because there is no fallback and therefore no knob | plan-b section 4a |
| Virtual-group cleanup + three-condition check + cleanup critical section | Prevent metadata bloat in temporary environments | Removed, entries are simply allowed to expire naturally | plan-b section 4b / 4c / 4e |
| Broker-side inheritance of parent-group config in `PopMessageProcessor:308` | To make a virtual group POP-able | Removed, because this plan uses a real group instead | plan-b section 3 change item 3 |
| New `consumerLabel` / snapshot fields on `PopMessageRequestHeader` | To send snapshots down to drive harvesting | Removed | plan-b section 3 change item 1 |
| Cold-start rebuild on master/slave switching | Needed because the harvesting service is in-memory derived state | Removed, because there is no harvesting service | plan-b section 4d |

**Result: zero broker code changes, except enabling the `enablePropertyFilter` configuration. All logic is concentrated on the Proxy consumer side.**

### 1.1 Explicit Trade-Offs

Removing fallback has a cost, and this plan accepts it explicitly:

- **Messages tagged with `label` while gray is offline or destroyed will accumulate on the `G%label` cursor and remain unconsumed.** Standard consumers will not harvest them.
- That backlog is handled by **natural topic TTL expiration**. The user explicitly chose "let it expire naturally."
- Real-group metadata for `G%label` is **retained permanently** and never reclaimed. If a temporary environment creates many labels, the number of group configs keeps growing. That is a known and accepted cost. If that becomes unacceptable, return to plan-b or use operational cleanup like plan A.

## 2. Locked Decisions (User Confirmed on 2026-06-30)

| # | Decision point | Choice | Impact |
|---|---|---|---|
| D1 | How the virtual group should exist on the broker side | **Proxy lazily creates a real group through admin** and inherits config from `G` | Zero broker code changes, avoids `GROUP_NOT_EXIST` traps |
| D2 | How the Proxy knows which label the request belongs to | **The consumer declares the label** through client properties or environment, and the Proxy reads it from request context | No awareness required in business logic |
| D3 | What to do with offline backlog and group metadata | **Default: let it expire naturally** through message TTL while the group is retained forever. **Optional timed cleanup** is provided behind a config switch that is OFF by default | Simplest default. Cleanup is an optional lightweight reclamation path, see section 2.1 |
| D4 | Where label translation happens | **Plan-A style transparent Proxy rewrite**. The consumer still connects as group `G` | Consumers remain unaware. Rewriting stays inside the Proxy |
| D5 | Whether producer-side labeling is part of this design | **No**. This plan assumes messages already carry `__RMQ_TRAFFIC_LABEL` | This document only covers consumer-side routing |
| D6 | Whether isolated-environment consumer discovery is used | **No**. The reference document is background only | Routing relies entirely on request-context label declaration |
| D7 | Default feature state for production safety | **Entire feature set OFF by default**, controlled by a master switch. Cleanup and routing logs each have separate switches, also OFF by default | Prevent accidental production use. Only isolated environments should enable it explicitly |

> The reference document in D6, [[2026-06-30-consumer-clientinfo-index-by-topic]], provides an event-driven consumer index. This plan **does not need discovery**. Each consumer declares its own label, and the Proxy can decide routing from the request alone without depending on whether some other label is online.

## 2.1 Configuration Items

Three `ProxyConfig` items are added. Their **default values make the entire feature set disabled and side-effect-free in production**.

| Config | Type | Default | Purpose | Behavior when disabled |
|---|---|---|---|---|
| `enableTrafficLabelRouting` | boolean | **`false`** | **Master switch**. Only when enabled does the Proxy read declared labels, rewrite groups, inject SQL92, and lazily create groups | Fully bypassed. All consumer-side requests follow native logic and forward `group=G` unchanged |
| `enableTrafficLabelGroupCleanup` | boolean | **`false`** | Periodically cleans isolated-environment metadata: subscription groups `G%label` plus retry queues `%RETRY%G%label` | No cleanup. `G%label` groups and retry topics remain forever |
| `enableTrafficLabelRoutingLog` | boolean | **`false`** | Emits routing-related logs for rewrite decisions, lazy group creation, and cleanup actions | No routing logs, to avoid production log noise |

### Master Switch Semantics (`enableTrafficLabelRouting`)

- **Disabled** (default, production): `ConsumerSideGroupRewriter`, `LabelRoutingResolver`, and `LabelGroupBootstrapper` all short-circuit. Consumer-side requests are not rewritten at all. This is **equivalent to the feature not existing**, which is the hard guarantee against accidental production usage.
- **Enabled** (isolated environments): the full routing logic in section 4 applies.
- When the master switch is disabled, the other two switches are meaningless because they are short-circuited by the master switch.

### Cleanup Switch Semantics (`enableTrafficLabelGroupCleanup`)

This reintroduces plan-b's cleanup capability in an **optional, lightweight** form, but **does not require plan-b's three-condition check**, because there is no standard harvesting in this plan and therefore no revive-orphan race between standard takeover and gray ownership.

```text
Periodic task (only when enableTrafficLabelRouting && enableTrafficLabelGroupCleanup):
  scan all real groups G%label
    for each G%label:
      if there is currently an online consumer for this label:
        skip
      else if origin offset == maxOffset
           and %RETRY%G%label offset == maxOffset
           and offline duration > cleanupIdleThresholdMs:
        admin deletes subscription group G%label + retry queue %RETRY%G%label
      else:
        skip
```

- The check is simpler than plan-b's: there is **no third condition for "revive has no in-flight checkpoint"**. This plan does not use standard harvesting. Gray's own revive loop stays closed within its own real group, so reading both offsets to the end is sufficient before deletion.
- An **offline-duration threshold** is still kept to avoid deleting groups during short flaps. A same-name rebuild can still take over as long as cleanup has not run.
- Cleanup uses existing admin RPCs, reusing `DeleteSubscriptionGroup` and the existing admin capability for deleting retry topics, so **the broker still requires no code changes**.
- Warning: this conflicts with the "same-name rebuild takes over backlog" behavior in section 6. Once cleanup deletes the group, a same-name gray rebuild starts again from the initial offset because the group and its cursor are gone. Therefore the cleanup threshold must be large enough, or cleanup should only be enabled after confirming the gray environment is truly destroyed. The default OFF setting avoids this tension.

## 3. Source-Level Conclusions That Shape the Landing Form

Comparing the design against Proxy source code produced four facts that directly define the final shape of this plan:

| Fact | Anchor | Impact |
|---|---|---|
| The **`createSubscriptionGroup` RPC client already exists**, sending `UPDATE_AND_CREATE_SUBSCRIPTIONGROUP` to the broker. The broker handler already exists as well, and it creates a **real persistent group** that syncs across master and slave automatically | `client/.../MQClientAPIImpl.java:431` (`createSubscriptionGroup`) | D1 is feasible. The Proxy only needs a thin forwarding method on `AdminService`, with **no broker changes** |
| The Proxy already holds an admin client, but the current `AdminService` interface **only exposes topic creation**, not group creation | `proxy/.../service/admin/DefaultAdminService.java:37`, `AdminService.java:23` | The only interface extension required is adding `createSubscriptionGroup(...)` to `AdminService` |
| **`ReceiptHandle.encode()` does not encode the group name**. It only carries startOffset, queueId, brokerName, offset, and similar fields. The group name used by ack and changeInvisibleTime is **re-read from the client request each time** | `common/.../consumer/ReceiptHandle.java:43-47`; `ReceiveMessageActivity.java:105` `request.getGroup().getName()` | **Critical constraint**: the client always sends `group=G`, so the Proxy must apply the same `G -> G%label` rewrite on **every consumer-side call**: receive, ack, changeInvisible, and DLQ. Otherwise ack goes to the wrong group and the message cannot be confirmed |
| Group extraction points are centralized: receive uses `ReceiveMessageActivity:105`, and ack / changeInvisible / DLQ use the same `request.getGroup()` pattern in their respective activities | `ReceiveMessageActivity.java:103-105` | The four rewrites can be converged into **one shared helper method** |

## 4. Component Design

### 4.1 Three Proxy Components

| Component | Responsibility | Hook point | State |
|---|---|---|---|
| **LabelRoutingResolver** | Given `(group=G, declaredLabel)`, produce `effectiveGroup` plus `SQL92 exp`. Gray becomes `G%gray1` + `__RMQ_TRAFFIC_LABEL='gray1'`. Standard with no label remains `G` + `__RMQ_TRAFFIC_LABEL IS NULL OR ='STANDARD'` | Utility class | Stateless |
| **ConsumerSideGroupRewriter** | Called uniformly at the four consumer-side entry points: receive, ack, changeInvisible, and DLQ. Rewrites the group name and injects or merges SQL92. **This guarantees consistency across all four paths**, which is required because `ReceiptHandle` carries no group name | `ReceiveMessageActivity` plus three ack-related activities | Stateless |
| **LabelGroupBootstrapper** | When routing to `G%gray1` for the first time, lazily creates the real group through `AdminService.createSubscriptionGroup(G%gray1, inherit config from G)`. Keeps an in-memory cache of already-created groups to avoid repeated admin calls | New thin method on `AdminService`, calling existing `MQClientAPIImpl.createSubscriptionGroup` | In-memory cache, disposable, at worst causing one extra idempotent create call after loss |

### 4.2 Component Boundary Diagram

```text
gray1 consumer --(group=G, declares label=gray1)--+
std   consumer --(group=G, no label)--------------+ 
                                                   v
+---------------------------- Proxy -----------------------------+
|                                                                |
|  +------------------------+   +------------------------------+ |
|  | LabelRoutingResolver   |   | ConsumerSideGroupRewriter   | |
|  | (group=G, label)       |-->| receive / ack /             | |
|  |   -> effectiveGroup    |   | changeInvisible / DLQ       | |
|  |   -> SQL92 exp         |   | unify group + filter rewrite| |
|  +------------------------+   +---------------+--------------+ |
|            | first time seeing G%gray1                        | |
|            v                                                  | |
|  +------------------------+                                   | |
|  | LabelGroupBootstrapper |                                   | |
|  | admin.createSubscription|                                  | |
|  | Group(G%gray1 inherits G)|                                 | |
|  | + created-group cache   |                                  | |
|  +------------------------+                                   | |
+-----------------------------------------------+---------------+
                | createSubscriptionGroup RPC   | POP/ACK group=G%gray1
                | existing call path            | exp=SQL92(label='gray1')
                v                               v
+----------------------- Broker (zero code changes) -----------------------+
| enablePropertyFilter=true (config only)                                  |
| G%gray1 is a real group, so findSubscriptionGroupConfig passes naturally |
| Independent topic@G%gray1 cursor reuses ConsumerOffsetManager            |
| Failed messages go to %RETRY%G%gray1 and reuse existing POP retry/revive |
+--------------------------------------------------------------------------+
```

### 4.3 Interface Contracts

| Boundary | Interface | Data |
|---|---|---|
| Consumer -> Proxy label declaration | gRPC client property / environment | `__RMQ_TRAFFIC_LABEL=gray1`, read from `ProxyContext` or `Settings` |
| Resolver -> Rewriter inside Proxy | Return value | `(effectiveGroup, sql92Exp)` |
| Proxy -> Broker for receive / ack / etc. | Existing RPC with rewritten group / exp | `group=G%gray1`, `exp=label='gray1'` |
| Proxy -> Broker for group creation | Existing `MQClientAPIImpl.createSubscriptionGroup` | `SubscriptionGroupConfig(groupName=G%gray1, inherit G config)` |

## 5. Label Declaration Channel and SQL92 Merge

- **Declaration channel**: the consumer declares the label through a gRPC client property `__RMQ_TRAFFIC_LABEL` or through environment. The Proxy reads it from `ProxyContext` or `Settings`. Business code remains unaware beyond startup configuration.
- **SQL92 merge correctness**: the consumer may already supply its own subscription filter expression. The resolver must **merge the label condition with AND**, not overwrite the consumer's original expression.
  - Gray: `(consumer origin exp) AND __RMQ_TRAFFIC_LABEL = 'gray1'`
  - Standard: `(consumer origin exp) AND (__RMQ_TRAFFIC_LABEL IS NULL OR __RMQ_TRAFFIC_LABEL = 'STANDARD')`
  - If the consumer has no original expression, use the label condition directly.
- **Broker config**: set `enablePropertyFilter=true` to enable SQL92 property filtering. This is a configuration change, not a code change.

## 6. Consumption Model and Path Outcomes

| Scenario | Path | Result |
|---|---|---|
| gray1 online | It POPs `G%gray1` using its own cursor | It receives only `label=gray1` messages |
| standard online | It POPs `G` with the SQL92 complement filter | It receives only `STANDARD` or null messages |
| gray1 retry after failure | Failed messages go into `%RETRY%G%gray1` | Reuses existing POP retry / revive with **zero special handling** |
| gray1 offline or destroyed | `label=gray1` messages accumulate on the `G%gray1` cursor | **Nobody consumes them**. There is no fallback and no harvesting. They expire via TTL |
| same-name gray1 rebuild | The real-group cursor for `G%gray1` still exists if cleanup has not deleted it | It **continues from backlog** instead of replaying from the beginning |
| metadata with cleanup OFF, default | Real group config `G%label` | **Retained forever** |
| metadata with cleanup ON, optional | Offline + both offsets drained + timeout threshold reached | Periodically deletes `G%label` and `%RETRY%G%label` |

## 7. Change List with Anchors

0. **`ProxyConfig` items**: add `enableTrafficLabelRouting`, `enableTrafficLabelGroupCleanup`, and `enableTrafficLabelRoutingLog`, all defaulting to `false`. Every component entry first checks the master switch and short-circuits when it is OFF.
   - Anchor: `proxy/.../config/ProxyConfig.java`
1. **`AdminService` and `DefaultAdminService`**: add `createSubscriptionGroup(String groupName, SubscriptionGroupConfig config)`, internally calling existing `MQClientAPIImpl.createSubscriptionGroup`. Cleanup also needs thin `deleteSubscriptionGroup` and retry-topic deletion wrappers over existing admin RPCs.
   - Anchors: `proxy/.../service/admin/AdminService.java:23`, `DefaultAdminService.java:37`, `client/.../MQClientAPIImpl.java:431`
2. **LabelRoutingResolver**: new utility class mapping `(group, label, originExp)` to `(effectiveGroup, mergedSql92)`.
3. **ConsumerSideGroupRewriter**: new shared rewriter invoked by all four consumer-side activities. **If `enableTrafficLabelRouting` is OFF, it passes through unchanged.**
   - Receive anchor: `proxy/.../grpc/v2/consumer/ReceiveMessageActivity.java:103-105`
   - Ack / changeInvisibleDuration / forwardToDLQ: their respective `request.getGroup()` extraction points
4. **LabelGroupBootstrapper**: new lazy-creation component with a created-group cache.
5. **LabelGroupCleaner**: optional periodic task that runs only when both `enableTrafficLabelRouting` and `enableTrafficLabelGroupCleanup` are ON. It scans, checks the conditions in section 2.1, and deletes groups and retry topics through admin RPCs.
6. **Routing logs**: emit logs for rewrite decisions, lazy group creation, and cleanup actions only when `enableTrafficLabelRoutingLog` is ON.
7. **Proxy startup wiring**: explicitly register and inject these components before `messagingProcessor.start()`. The Proxy has no SPI, so there is no automatic discovery. The cleanup task must start and stop together with the Proxy lifecycle.
   - Startup registration pattern reference: [[2026-06-30-consumer-clientinfo-index-by-topic]]
8. **Broker configuration**: enable `enablePropertyFilter=true`. This is a deployment change, not a code change.

> **No broker code is modified.** Compared with plan-b's multi-point broker changes, this plan has zero broker change items.

## 8. Pros and Cons

**Pros**
- **Zero broker code changes**. Only `enablePropertyFilter` needs to be enabled.
- **Production-safe by default**. With `enableTrafficLabelRouting` OFF, the feature is completely bypassed and behaves exactly as if it did not exist.
- **Reuses existing capabilities**. Real groups use `topic@group` accounting, existing POP retry / revive, and existing admin RPCs for group creation.
- **No fallback machinery**. There is no online snapshot, harvesting, grace period, or master/slave cold-start rebuild, so the complexity is dramatically lower than plan-b.
- **Transparent to consumers**. Consumers still connect as group `G` and only declare a label. Group rewriting, SQL92 injection, and lazy group creation all stay in the Proxy.
- **Same-name rebuild naturally takes over**. The cursor is persistent and cleanup is OFF by default, so a rebuild continues directly from backlog.
- **Cleanup remains optional**. `enableTrafficLabelGroupCleanup` can be enabled only when lightweight reclamation is needed.

**Cons / Risks**
- **No fallback**. Messages for a label are left unconsumed when gray is offline or destroyed. This plan relies on TTL expiration.
- **Cleanup vs takeover tension**. If cleanup is enabled, deleting the group removes the cursor, so a same-name rebuild restarts from the initial offset.
- **No metadata reclamation when cleanup is OFF**. With many labels, `G%label` group config count keeps growing.
- **Rewrite consistency across four paths is mandatory**. Receive, ack, changeInvisible, and DLQ must all use the exact same rewrite logic, or ack goes to the wrong group and confirmation breaks.
- **SQL92 merge must be correct**. The label expression must be AND-merged rather than replacing the consumer's original expression.
- **Short coexistence window during rebuild**. If standard and gray briefly coexist while the consumer is rebuilding, exact SQL92 label matching still isolates them, so cross-environment leakage does not occur.

## 9. Relationship to Other Plans

| Plan | Fallback | Broker changes | Complexity | Fit |
|---|---|---|---|---|
| This plan C (Proxy-only, no fallback) | No | **0** | Low | Isolated environments can accept backlog expiring naturally and want minimum intrusion |
| [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] | Yes, dynamic | Multiple | Medium | Fallback is required, especially in temporary environments |
| [[2026-06-29-traffic-label-routing-plan-a-strict-bypass]] | Yes | Multiple | High | Label count is so large that virtual-group metadata becomes too expensive |

> The real split in plan choice is: **do you require "messages fall back to standard after gray goes offline"?**
> - If yes, choose plan-b.
> - If no, and you can accept backlog expiring naturally, choose this Proxy-only plan C.

## 10. E2E and API Test Cases

> Per planning rules, every core user flow should have at least one E2E test. Tests should not mock dependent services. Use a real Broker + Proxy setup, such as Testcontainers or a local deployment, with `enablePropertyFilter=true` enabled on the broker.

### 10.1 Core Flow Test Matrix

| ID | Flow | Core assertion |
|---|---|---|
| E2E-01 | Isolated consumption while gray is online | gray1 consumes only `label=gray1`, standard consumes only `STANDARD` or null, with no cross-over |
| E2E-02 | Isolation with multiple gray environments online | gray1, gray2, and standard remain completely isolated |
| E2E-03 | Lazy group creation | The first gray1 connection triggers admin creation of real group `G%gray1`, inheriting config from `G` |
| E2E-04 | Retry isolation after failure | gray1 failed messages go to `%RETRY%G%gray1`, gray1 consumes its own retries, and standard retry is not polluted |
| E2E-05 | Same-name rebuild takes over backlog | gray1 goes offline with backlog, then same-name rebuild resumes from the existing cursor instead of replaying from the beginning |
| E2E-06 | Offline backlog never falls back (negative assertion) | After gray1 goes offline, standard **never consumes** `label=gray1` messages |
| E2E-07 | Ack consistency | After gray1 consumes a message, ack returns to `G%gray1` correctly and the message is not redelivered |
| E2E-08 | Master switch OFF yields pure bypass | With `enableTrafficLabelRouting=false`, declared labels have no effect, no rewrite happens, and no lazy group is created |
| E2E-09 | Optional cleanup | When cleanup is enabled, destroyed gray groups and retry topics are deleted after the threshold, while online gray groups are preserved |

### 10.2 E2E-01: Isolated Consumption While Gray Is Online

```text
Prerequisites: Broker + Proxy running, enablePropertyFilter=true, topic=test-topic, queueNum=4
  - gray1 consumer: connects as group=G, declares label=gray1
  - standard consumer: connects as group=G, with no label

Steps:
  1. Bring both consumers online
  2. Produce 10 messages with label=gray1, 10 with label=STANDARD, and 10 with no label

Assertions:
  - gray1 consumption count = 10, all with label=gray1
  - standard consumption count = 20, covering STANDARD + no-label messages
  - No cross-over: gray1 consumes no STANDARD/null messages, and standard consumes no gray1 messages
```

### 10.3 E2E-03: Lazy Group Creation

```text
Steps:
  1. Confirm that broker has no group G%gray1 yet, for example through mqadmin
  2. Let the gray1 consumer connect for the first time as group=G with label=gray1
  3. Proxy triggers LabelGroupBootstrapper

Assertions:
  - Broker now contains the real subscription group G%gray1
  - G%gray1 inherits key config from G, such as retryMaxTimes and retryQueueNum
  - A second gray1 connection does not issue another create-group RPC because the local cache is hit
```

### 10.4 E2E-05: Same-Name Rebuild Takes Over Backlog

```text
Steps:
  1. Bring gray1 online and let it consume M1-M10
  2. Stop gray1 with the cursor at M10
  3. Produce 20 more gray1 messages, M11-M30, which accumulate on G%gray1
  4. Wait for a while. There is no grace-period concept in this plan, so this is only observational
  5. Assert that standard still consumes none of M11-M30 because there is no fallback
  6. Rebuild gray1 with the same group=G and label=gray1
  7. Wait until consumption completes

Assertions:
  - gray1 resumes from M11 because the cursor was not reset
  - gray1 does not replay from M1
  - standard consumption count for M11-M30 = 0
  - Produced total 30 = gray1 consumed total 30
```

### 10.5 E2E-06: Offline Backlog Never Falls Back

```text
Steps:
  1. Bring gray1 and standard online
  2. Stop gray1 without rebuilding it
  3. Produce 15 messages with label=gray1
  4. Keep waiting beyond any typical grace-period window

Assertions:
  - standard consumption count for label=gray1 = 0 forever
  - The 15 messages remain on the G%gray1 cursor until TTL expiration
  - Standard consumption of STANDARD/null messages is unaffected
```

### 10.6 E2E-07: Ack Consistency

```text
Steps:
  1. Bring gray1 online as group=G with label=gray1
  2. Produce 5 messages with label=gray1
  3. Let gray1 consume and ack all 5 messages
  4. Wait longer than invisibleTime

Assertions:
  - The 5 messages are not redelivered because ack returned to G%gray1 rather than G
  - The G%gray1 cursor advances to max
  - Validation point: captured ack RPC shows group field = G%gray1
```

### 10.8 E2E-08: Master Switch OFF Means Full Bypass

```text
Prerequisite: enableTrafficLabelRouting=false (default)

Steps:
  1. A gray1 consumer connects as group=G and declares label=gray1
  2. Produce 10 messages with label=gray1 and 10 with label=STANDARD

Assertions:
  - Proxy performs no group rewrite at all, and captured RPCs always show group = G
  - Lazy group creation is never triggered, and broker still has no G%gray1
  - gray1 and standard both consume through the native shared cursor on group=G
  - Behavior is identical to the system without this feature
```

### 10.9 E2E-09: Optional Cleanup

```text
Prerequisites:
  enableTrafficLabelRouting=true
  enableTrafficLabelGroupCleanup=true
  cleanupIdleThresholdMs=10s
  cleanup scan interval=5s

Steps:
  1. Let gray1 consume all its label=gray1 messages so both G%gray1 and %RETRY%G%gray1 offsets reach max
  2. Stop gray1 and do not rebuild it
  3. Wait for cleanupIdleThresholdMs plus one scan cycle

Assertions:
  - Subscription group G%gray1 is deleted
  - Retry queue %RETRY%G%gray1 is deleted

Control case against false deletion:
  4. Keep gray2 online
  5. Assert that G%gray2 is not cleaned because an online consumer still exists

Flap control:
  6. Let gray3 come online, consume all messages, go offline for 5s, which is below the threshold, then rebuild
  7. Assert that G%gray3 was not deleted and the rebuild resumes from the existing cursor
```

### 10.10 API Tests: Consumer-Side Request Rewrite Contract

| Case | Client request | Expected Proxy -> Broker request |
|---|---|---|
| Gray receive | `group=G`, declared `label=gray1`, empty exp | `group=G%gray1`, `exp=__RMQ_TRAFFIC_LABEL='gray1'` |
| Standard receive | `group=G`, no label, empty exp | `group=G`, `exp=__RMQ_TRAFFIC_LABEL IS NULL OR ='STANDARD'` |
| Gray receive with original filter | `group=G`, `label=gray1`, `exp=a>1` | `group=G%gray1`, `exp=(a>1) AND __RMQ_TRAFFIC_LABEL='gray1'` |
| Gray ack | `group=G`, handle with no group name inside | ack RPC uses `group=G%gray1` |
| Gray changeInvisible | `group=G`, handle present | RPC uses `group=G%gray1` |
| Lazy group creation | First gray1 receive | `createSubscriptionGroup(G%gray1)` is sent before POP |
| Master switch OFF | `enableTrafficLabelRouting=false`, gray1 receive | Original `group=G` is passed through, with no rewrite and no lazy create-group RPC |
