---
name: traffic-label-routing-plan-c-proxy-only
description: Plan C - Proxy-only dynamic routing driven by ConsumerIdsChangeListener, with standard-side subscription filters assembled from live isolated-consumer topology
date: 2026-06-30
status: brainstorming
verified: verified against source (ReceiptHandle / ReceiveMessageActivity / DefaultAdminService / MQClientAPIImpl / ConsumerManager / ConsumerIdsChangeListener)
---

# Plan C: Proxy-Only Dynamic Routing from Consumer Index

> This document keeps the target of **Proxy-only changes with zero broker code changes**, but the routing basis is no longer "consumer declares a label on every request".
>
> Route information comes from the event-driven index in [[2026-06-30-consumer-clientinfo-index-by-topic]].
> The key extension in this revision is:
>
> - gray consumers still land on real groups `G%label`
> - standard consumers stay on `G`
> - when standard consumers assemble `subscriptionFilter`, the Proxy dynamically excludes **currently online isolated labels** for the same `(logicalGroup, topic)`
> - that exclusion condition is retrieved from a table maintained inside `ConsumerIdsChangeListener`, so filter assembly is O(1) on the hot path

> This is still **not** plan-b style harvesting. It gives dynamic routing for **current and future traffic**, but does **not** reclaim historical messages already skipped by standard group `G`.

## 0. One-Sentence Positioning

> Each isolated environment still maps to one **real subscription group** `G%label`, with its own cursor and retry topic.
> The Proxy rewrites registration once, builds a live `topic -> logicalGroup -> isolatedLabelSet / clientId` index from `ConsumerIdsChangeListener`, and then:
>
> - resolves gray consumer runtime calls to `G%label`
> - keeps standard consumers on `G`
> - dynamically assembles standard-side SQL92 as "exclude currently online isolated labels for this topic and logical group"

> There is **no backlog harvesting**. If standard group `G` already skipped old gray messages while gray was online, those old messages stay on `G%label` until TTL expiration or same-name gray rebuild.

## 1. What Changed Compared with the Previous Plan-C

The previous version of plan-c used this model:

- gray consumers connected as logical group `G`
- gray consumers carried `label=gray1` on each runtime request
- Proxy translated `(group=G, label=gray1)` into `(group=G%gray1, SQL92=label='gray1')`
- standard group `G` used a fixed complement filter such as `STANDARD/null`

That was simple, but it had two weaknesses:

1. the runtime route source was too request-local
2. standard-side filtering could not reflect the **current isolated-consumer topology** under `(group, topic)`

This revision changes the core routing basis:

| Aspect | Old plan-c | Revised plan-c |
|---|---|---|
| Runtime route source | Request-context label declaration | **Dynamic index from `ConsumerIdsChangeListener`** |
| Standard filter | Fixed `STANDARD/null` style complement | **Dynamic exclusion of currently online isolated labels** |
| Data source for hot-path filter assembly | Request-local metadata | **Precomputed table inside the listener/index** |
| Registration path | Mostly implicit | **Explicitly rewritten once so broker-side registration lands on `G` or `G%label`** |

## 2. Core Design Decision

The route information source is now the consumer index from [[2026-06-30-consumer-clientinfo-index-by-topic]], but the index needs one extra derived view for fast standard-filter assembly.

### 2.1 Two Views Maintained from the Same Event Stream

`ConsumerIdsChangeListener` maintains two related views:

1. **Gray route view**
   - key: `(topic, logicalGroup, clientId)`
   - value: `effectiveGroup = G%label`
   - purpose: resolve gray receive / ack / changeInvisible / DLQ calls to the right real group

2. **Standard filter view**
   - key: `(topic, logicalGroup)`
   - value: `Set<label>` of **currently online isolated labels**, or a directly cached SQL92 fragment
   - purpose: let standard consumers assemble `subscriptionFilter` quickly on the receive path

This means the listener is no longer only a "topic -> clientId" index. It becomes the **single event-driven topology cache** used by both gray-side group rewrite and standard-side filter assembly.

### 2.2 Why the Derived Standard Filter Table Is Needed

The user request is correct: on the standard-consumer path, when assembling `subscriptionFilter`, the Proxy must know:

- for this logical group `G`
- on this topic `T`
- which isolated environments currently have live consumers

Then the standard filter should **exclude those active isolated labels**.

Doing that by scanning all client registrations on every receive would put an avoidable cost on the hot path. So the right shape is to maintain a derived table inside the listener/index and let standard filter assembly read it directly.

## 3. Locked Decisions

| # | Decision point | Choice | Impact |
|---|---|---|---|
| D1 | How isolated groups exist on the broker | **Proxy lazily creates real groups `G%label` through admin**, inheriting config from `G` | Zero broker code changes, avoids `GROUP_NOT_EXIST` |
| D2 | What the runtime route source is | **`ConsumerIdsChangeListener`-driven topology cache** | Runtime routing follows live registration state |
| D3 | How standard-side filtering is assembled | **From a derived `(topic, logicalGroup) -> activeIsolatedLabels/sql92` table** | Standard receive can get exclusion conditions in O(1) |
| D4 | Where explicit label declaration still exists | **Registration / bootstrap only** | The very first placement of a gray consumer still needs one bootstrap label |
| D5 | Whether this introduces plan-b harvesting | **No** | Dynamic routing is about current/future traffic only, not backlog take-over |
| D6 | Whether producer-side labeling is part of this document | **No**. Messages are assumed to already carry `__RMQ_TRAFFIC_LABEL` | This plan is consumer-routing only |
| D7 | Feature default state | **Master switch OFF by default** | Production-safe |

## 4. What Is Still Removed Compared with Plan-B

Removing plan-b-style fallback still eliminates most of the complexity:

| Plan-b mechanism | Why it exists in plan-b | This plan |
|---|---|---|
| Online snapshot for offline-label harvesting | To know which gray backlog standard must take over | Removed |
| Harvest scheduler + standard fan-in | To pull offline gray backlog into standard consumption | Removed |
| Grace period | To arbitrate "gray rebuild takes over vs standard harvests" | Removed |
| Three-condition cleanup with revive guard | To reclaim harvested virtual groups safely | Simplified |
| Broker-side virtual-group compensation | To allow broker-side virtual groups | Removed, because Proxy creates **real** groups |

## 5. Source-Level Conclusions That Shape the Landing Form

Comparing the design against source code gives these key facts:

| Fact | Anchor | Impact |
|---|---|---|
| `ConsumerManager.registerConsumer()` emits `CLIENT_REGISTER` with both `ClientChannelInfo` and `Set<String> topics` | `broker/.../ConsumerManager.java:252-253` | This is the exact event stream needed to build both gray-route and standard-filter views |
| `doChannelCloseEvent()` and `unregisterConsumer()` emit `CLIENT_UNREGISTER` with group + topics | `broker/.../ConsumerManager.java:155`, `177`, `315` | The listener can remove stale routes and stale active-label membership |
| `MessagingProcessor.registerConsumerListener()` is the official hook | Referenced in [[2026-06-30-consumer-clientinfo-index-by-topic]] | Proxy can install the listener/index before startup |
| `ReceiptHandle.encode()` does not encode group name | `common/.../ReceiptHandle.java:43-47` | Receive / ack / changeInvisible / DLQ must all resolve the same `effectiveGroup` from the shared cache |
| `createSubscriptionGroup` RPC already exists | `client/.../MQClientAPIImpl.java:431` | Proxy can lazily create `G%label` with no broker code changes |

## 6. Component Design

### 6.1 Five Proxy Components

| Component | Responsibility | Hook point | State |
|---|---|---|---|
| **RegistrationGroupRewriter** | On consumer registration / heartbeat, translate logical group `G` plus bootstrap label into `G` or `G%label` | Registration path before broker-side `registerConsumer` | Stateless |
| **TopicClientInfoIndex** | Listen to `CLIENT_REGISTER` / `CLIENT_UNREGISTER` and maintain gray-route and standard-filter topology | Registered via `MessagingProcessor.registerConsumerListener(...)` | In-memory, event-driven |
| **DynamicRouteResolver** | Resolve gray runtime `effectiveGroup` from `(logicalGroup, topic, clientId)` or fallback reverse lookup | Shared utility | Read-only view over index |
| **StandardFilterAssembler** | Given `(logicalGroup, topic, originExp)`, read active isolated labels from the precomputed table and assemble the standard exclusion SQL92 | Receive path only | Stateless |
| **ConsumerSideGroupRewriter** | For receive / ack / changeInvisible / DLQ, coordinate `DynamicRouteResolver` and `StandardFilterAssembler` and rewrite the outgoing broker request | Four consumer-side activities | Stateless |
| **LabelGroupBootstrapper** | Lazily create real group `G%label` before first registration or first routed use | `AdminService.createSubscriptionGroup(...)` | In-memory dedup cache |

### 6.2 Topology Tables Maintained by the Listener

Inside `TopicClientInfoIndex`, maintain at least these structures:

```text
topicGroupClientTable:
  Map<topic, Map<logicalGroup, Map<effectiveGroup, Set<clientId>>>>

clientRouteTable:
  Map<clientId, Set<effectiveGroup>>

activeIsolatedLabelTable:
  Map<topic, Map<logicalGroup, Set<label>>>

optional cachedStandardSql92Table:
  Map<topic, Map<logicalGroup, String>>
```

The important point is not the exact Java type. The important point is:

- the listener handles all register/unregister churn once
- receive-path filter assembly performs only table lookup plus string composition

### 6.3 Component Boundary Diagram

```text
gray1 consumer --(group=G, bootstrap label=gray1 on registration)--+
standard consumer --(group=G, no bootstrap label)------------------+
                                                                    v
+----------------------------- Proxy --------------------------------------+
|                                                                           |
|  +---------------------------+     +-----------------------------------+ |
|  | RegistrationGroupRewriter |---->| broker registerConsumer(group=G%gray1) |
|  +---------------------------+     +-----------------------------------+ |
|                                                                           |
|  +---------------------------+     +-----------------------------------+ |
|  | TopicClientInfoIndex      |<----| ConsumerIdsChangeListener events  | |
|  | gray-route table          |     | CLIENT_REGISTER / UNREGISTER      | |
|  | standard-filter table     |     +-----------------------------------+ |
|  +-------------+-------------+                                           |
|                |                                                         |
|      +---------+----------+                          +-----------------+ |
|      | DynamicRouteResolver|                         |StandardFilter   | |
|      | resolve gray group  |                         |Assembler        | |
|      +---------+----------+                          +--------+--------+ |
|                |                                              |          |
|                +----------------+-----------------------------+          |
|                                 v                                        |
|                   +-------------------------------+                      |
|                   | ConsumerSideGroupRewriter     |                      |
|                   | receive / ack / changeInvisible / DLQ               |
|                   +----------------+--------------+                      |
|                                    |                                     |
|                   +----------------v--------------+                      |
|                   | LabelGroupBootstrapper        |                      |
|                   | ensure real group G%gray1     |                      |
|                   +----------------+--------------+                      |
+------------------------------------|--------------------------------------+
                                     v
+---------------------- Broker (zero code changes) ------------------------+
| real group G%gray1 exists through admin                                   |
| ConsumerManager emits register/unregister events with group + topics      |
| topic@group accounting and %RETRY%G%gray1 retry semantics are native     |
+--------------------------------------------------------------------------+
```

## 7. Registration and Runtime Rules

### 7.1 Registration Path

Registration is now explicitly part of the design:

1. Consumer connects using logical group `G`
2. If it is a gray consumer, it supplies a **bootstrap label once** at registration / heartbeat
3. Proxy rewrites registration to `G%label`
4. Proxy lazily creates `G%label` if needed
5. Broker records the consumer under `G%label`
6. Listener receives `CLIENT_REGISTER` and updates:
   - gray route membership
   - active isolated labels for each `(topic, logicalGroup)`
   - optional cached standard SQL92 fragment

This is the only place where explicit label declaration still exists.

### 7.2 Gray Runtime Path

For gray receive / ack / changeInvisible / DLQ:

1. Proxy reads logical group `G`
2. Proxy extracts `clientId`
3. Proxy resolves `effectiveGroup` from the index
4. If resolved to `G%gray1`, Proxy rewrites the broker request to `G%gray1`
5. SQL92 is derived from the group suffix:
   - `__RMQ_TRAFFIC_LABEL = 'gray1'`

### 7.3 Standard Runtime Path

For standard receive:

1. Proxy keeps the broker-side group as `G`
2. Proxy looks up `(topic, logicalGroup=G)` in the derived standard-filter table
3. Proxy gets the set of currently online isolated labels, for example `{gray1, gray2}`
4. Proxy assembles SQL92 that **excludes those currently online isolated labels**
5. Proxy merges that exclusion expression with the consumer's original subscription expression using **AND**

So the hot path is:

```text
(topic, logicalGroup) -> activeIsolatedLabels or cachedSql92 -> merge with originExp -> broker request on group G
```

### 7.4 Standard SQL92 Shape

The exact SQL92 string can be generated either on demand or cached in the listener. Conceptually, for active isolated labels `{gray1, gray2}`, the standard-side meaning is:

```text
exclude gray1 and gray2 from standard consumption
```

One acceptable generated shape is:

```text
(__RMQ_TRAFFIC_LABEL IS NULL
 OR (__RMQ_TRAFFIC_LABEL <> 'gray1' AND __RMQ_TRAFFIC_LABEL <> 'gray2'))
```

If the project wants to preserve explicit `STANDARD` semantics, the generated filter can include that explicitly as well. The important part is not the exact string form. The important part is:

- it is derived from the **current active isolated label set**
- it is assembled quickly from the listener-maintained table
- it changes automatically when isolated consumers register or unregister

## 8. Consumption Semantics

| Scenario | Path | Result |
|---|---|---|
| gray1 online | registered on `G%gray1`, runtime gray requests resolve to `G%gray1` | gray1 consumes `label=gray1` |
| standard online while gray1 online | standard stays on `G`, filter excludes `gray1` | standard does not consume current gray1 traffic |
| gray1 disconnects | listener removes gray1 from active isolated labels | standard filter recomputes and no longer excludes `gray1` for **future messages** |
| old gray1 messages already skipped by `G` | remain accounted on `G%gray1` | **not harvested** by this plan |
| gray1 same-name rebuild | registration again lands on `G%gray1`; cursor persists if not cleaned | gray1 can take over its own backlog |
| retry after gray failure | `%RETRY%G%gray1` remains isolated | native retry/revive reuse |
| cleanup OFF | `G%label` groups remain forever | simplest mode |
| cleanup ON | offline + drained + threshold reached | periodic deletion of `G%label` and `%RETRY%G%label` |

> This is the most important semantic boundary in this plan:
> **dynamic standard filtering affects future routing, not historical backlog reclamation.**

## 9. Change List

0. **`ProxyConfig` items** remain:
   - `enableTrafficLabelRouting`
   - `enableTrafficLabelGroupCleanup`
   - `enableTrafficLabelRoutingLog`
1. **`AdminService`** keeps `createSubscriptionGroup(...)` and deletion helpers
2. **RegistrationGroupRewriter**: rewrite gray registration / heartbeat to `G%label`
3. **TopicClientInfoIndex**:
   - implement the event-driven index from [[2026-06-30-consumer-clientinfo-index-by-topic]]
   - add derived standard-filter table keyed by `(topic, logicalGroup)`
4. **DynamicRouteResolver**: resolve gray-side runtime group from the index
5. **StandardFilterAssembler**: build standard-side exclusion SQL92 from the derived table
6. **ConsumerSideGroupRewriter**:
   - gray path: rewrite group to `G%label`
   - standard path: keep group `G` and rewrite `subscriptionFilter`
7. **LabelGroupBootstrapper**: keep lazy creation of `G%label`
8. **Optional cleanup**: judge online/offline from the same listener-maintained topology cache
9. **Proxy startup wiring**:
   - register `TopicClientInfoIndex` before startup
   - wire registration rewrite to the registration path
   - wire standard filter assembly to the receive path
   - wire gray runtime group resolution to receive / ack / changeInvisible / DLQ
10. **Broker config**: keep `enablePropertyFilter=true`

## 10. Pros and Risks

**Pros**

- still **zero broker code changes**
- standard-side routing is now **dynamic per `(topic, logicalGroup)`**
- hot-path filter assembly is cheap because the listener maintains a ready-to-read table
- gray-side and standard-side routing both come from the **same event source**
- same-name rebuild remains natural because `G%label` is a real persistent group

**Risks / Caveats**

- a **bootstrap label is still required once** at registration time
- registration rewrite and runtime gray resolution must remain strictly consistent
- standard dynamic filtering is **not** plan-b harvesting
  - it only affects messages that standard group `G` has not already skipped
  - it does not reclaim old gray backlog from `G%label`
- cached SQL92 strings must be updated correctly on register/unregister churn
- some runtime RPCs may not carry topic, so gray-side resolution still needs reverse `clientId -> effectiveGroup` support

## 11. Relationship to Other Plans

| Plan | Standard-side behavior | Gray-side behavior | Historical backlog handoff | Broker changes |
|---|---|---|---|---|
| This plan C | `G` dynamically excludes active isolated labels | gray runtime calls resolve to `G%label` from listener index | No | **0** |
| Earlier plan-c | `G` used fixed complement filter | gray runtime calls used per-request label declaration | No | **0** |
| [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]] | standard harvests offline `G%label` | gray and standard both route through harvesting logic | Yes | Multiple |

## 12. E2E and API Test Cases

### 12.1 Core Flow Matrix

| ID | Flow | Core assertion |
|---|---|---|
| E2E-01 | Gray registration populates routing tables | after gray1 registers, listener contains gray route and active isolated label membership |
| E2E-02 | Standard receive gets dynamic exclusion filter | when gray1 is online, standard filter excludes `gray1` |
| E2E-03 | Dynamic filter updates on disconnect | when gray1 disconnects, standard filter no longer excludes `gray1` |
| E2E-04 | Gray runtime ack consistency | gray ack resolves `G%gray1` from the listener index |
| E2E-05 | Same-name rebuild reuses group and route | re-registration restores `G%gray1` route and same cursor |
| E2E-06 | No historical backlog harvesting | old gray backlog on `G%gray1` is not consumed by standard after gray disconnects |

### 12.2 E2E-02: Standard Receive Gets Dynamic Exclusion Filter

```text
Prerequisites:
  - Broker + Proxy running
  - topic=test-topic
  - gray1 registered as logical group G with bootstrap label gray1
  - standard registered as logical group G

Steps:
  1. gray1 registration updates activeIsolatedLabelTable(topic, G) = {gray1}
  2. standard issues receive on topic
  3. Proxy assembles standard subscriptionFilter from the derived table

Assertions:
  - broker-side group remains G
  - SQL92 excludes gray1
  - standard does not consume current gray1 traffic
```

### 12.3 E2E-03: Dynamic Filter Updates on Disconnect

```text
Steps:
  1. gray1 is online, so standard filter excludes gray1
  2. gray1 disconnects
  3. listener receives CLIENT_UNREGISTER and removes gray1 from active labels
  4. standard issues another receive on the same topic

Assertions:
  - assembled standard filter no longer excludes gray1
  - newly arriving gray1-labeled messages can now be matched by standard
  - old messages already skipped by G are not replayed
```

### 12.4 E2E-04: Gray Ack Consistency

```text
Steps:
  1. gray1 registers as logical group G with bootstrap label gray1
  2. gray1 receives messages
  3. gray1 acks without carrying request-local label

Assertions:
  - ack resolves to G%gray1 from the listener-maintained client route table
  - no message is redelivered after invisibleTime
```

### 12.5 E2E-06: No Historical Backlog Harvesting

```text
Steps:
  1. gray1 is online and standard filter excludes gray1
  2. some gray1 messages are consumed only by G%gray1, while standard G skips them
  3. gray1 disconnects
  4. standard filter recomputes and stops excluding gray1

Assertions:
  - new gray1 messages may be matched by standard
  - old gray1 backlog already sitting on G%gray1 is not harvested by standard
  - this remains outside plan-c and is exactly the boundary to plan-b
```

### 12.6 API Contract

| Case | Client request | Expected Proxy -> Broker behavior |
|---|---|---|
| Gray register | logical `group=G`, bootstrap label `gray1` | register as `G%gray1`, update listener tables |
| Standard register | logical `group=G`, no label | register as `G` |
| Standard receive while gray1 online | `group=G`, topic=T | keep broker group `G`, attach SQL92 that excludes gray1 |
| Standard receive after gray1 offline | `group=G`, topic=T | keep broker group `G`, attach SQL92 recomputed without gray1 exclusion |
| Gray receive | `group=G`, clientId belongs to gray1 route | rewrite group to `G%gray1` |
| Gray ack | `group=G`, no request-local label | resolve `G%gray1` from client route table |
| Master switch OFF | routing disabled | both registration and runtime pass through unchanged |
