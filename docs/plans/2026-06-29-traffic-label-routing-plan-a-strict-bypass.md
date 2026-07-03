---
name: traffic-label-routing-plan-a-strict-bypass
description: Plan A - strict isolation plus bypass storage: copy isolated messages before skipping them, and let isolated consumers read from the bypass path
date: 2026-06-29
status: brainstorming
---

# Plan A: Strict Isolation plus Bypass Storage

> See [[2026-06-29-traffic-label-routing-design]] for the top-level design.
> For comparison, see [[2026-06-29-traffic-label-routing-plan-b-subcursor-harvest]].

## 1. Core Idea

In the POP model, skipping a message on a single cursor is one-way. Plan A solves this by **copying an isolated message into a bypass store before the standard cursor skips it**, so that consumers in the isolated environment read their own messages from the bypass path while the standard cursor keeps moving forward normally. That way, the cursor on the original physical queue is always monotonic and never blocked by any environment.

In one sentence: **trade space, meaning one extra copy, for monotonic cursor advancement**.

## 2. Semantics

- For isolated messages `label==grayX`:
  - If grayX is **online**, copy the message into grayX's bypass store, let grayX consume it there, and let the standard cursor skip over it.
  - If grayX is **offline**, do not copy it. Deliver it directly to standard consumers as fallback.
- For standard messages `label==STANDARD`, deliver them directly to standard consumers.

"Online" and "offline" are determined by the **traffic-label online snapshot** sent down by the Proxy.

## 3. Data Flow

```text
producer(gray1) --B(label=gray1)--> physical queue
                                      |
                 standard consumer POP| scans B
                                      | snapshot says gray1 is online
                                      v
                             copy B into bypass(gray1 only)
                                      | standard cursor skips B
                                      v
                 gray1 consumer POP -> bypass(gray1) reads B
```

## 4. Key Change Points

### 4.1 Proxy

- Maintain the traffic-label online snapshot through `ClusterConsumerManager` plus `HeartbeatSyncer`.
- Include `consumerLabel` plus the online snapshot, or a snapshot version cached broker-side, on POP requests.

### 4.2 Broker / `PopMessageProcessor`

- Make the per-message routing decision by reading the `__RMQ_TRAFFIC_LABEL` property and combining it with the snapshot to decide **deliver / copy to bypass / skip**.
- Perform the bypass write **before** `appendCheckPoint`. If copying fails, do not advance the cursor, so that no message is lost.

### 4.3 Bypass Storage Shape

Two candidates need evaluation:

- **Route topic**: reuse the old plan's idea of a system topic with `routeKey=group/topic/label`. Mature, but heavy.
- **Per-label logical queue**: one logical queue per label. Semantically cleaner.

### 4.4 Revive / Retry Special Handling

- Messages in the bypass path have their own checkpoint, ack, and changeInvisibleTime lifecycle.
- `PopReviveService` must recognize that a message came from bypass storage and send retries back to bypass rather than the original queue. **This is the main source of complexity in plan A.**

## 5. Advantages

- **High fallback timeliness**: the bypass path delivers independently, so isolated consumers are not constrained by the standard cursor.
- **Clear isolation boundaries**: each environment reads from its own bypass path, giving strong physical isolation.
- **Dead environments never block the main queue**: the main queue cursor always moves forward.

## 6. Drawbacks and Risks

- **Write amplification**: every isolated message that is online gets copied once.
- **Bypass consistency**: "copy first, then skip the original cursor" must be guaranteed, or messages will be lost. Cross-store consistency needs careful handling.
- **Complex revive special handling**: the retry path must distinguish bypass storage from the main queue, which drives deeper changes into `PopReviveService`.
- **Bypass metadata overhead**: both per-label queues and route topics introduce extra offset and checkpoint management.

## 7. Relationship to the Old Plan

The older [[2026-06-26-traffic-label-routing-server-side-pop-retry]] plan was essentially one concrete implementation of plan A, using route topics, DELIVER/YIELD/MOVE actions, and revive special handling. This plan extracts the "copy to bypass" core idea, but **does not hard-bind the design to route topics by default**. The final bypass shape remains an evaluable sub-option.

## 8. Suitable Scenarios

- Isolated environments are **temporary and frequently destroyed**, such as PR preview or load-test environments. The bypass path lets messages from dead environments fall back to standard immediately, without affecting the main queue.
- Businesses are **sensitive to fallback delay**.
