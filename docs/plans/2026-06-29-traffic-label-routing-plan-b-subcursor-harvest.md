---
name: traffic-label-routing-plan-b-subcursor-harvest
description: Plan B - virtual subscription-group sub-cursors plus standard harvesting: carry each traffic label in a virtual group G%label, and let standard consumers take over offline isolated labels
date: 2026-06-29
status: brainstorming
verified: verified against source (ConsumerOffsetManager / PopMessageProcessor / LMQ path)
---

# Plan B: Virtual Subscription-Group Sub-Cursors plus Standard Harvesting

> See [[2026-06-29-traffic-label-routing-design]] for the top-level design.
> For comparison, see [[2026-06-29-traffic-label-routing-plan-a-strict-bypass]].

## 0. Source-Level Conclusions

Comparing the design against RocketMQ source code produced three facts that directly reshape this plan:

| Fact | Anchor | Impact on the plan |
|---|---|---|
| Offset accounting key is `topic@group`, with one cursor per `(group, queueId)` | `ConsumerOffsetManager.java:201,241` | The cleanest implementation of "per-label sub-cursors" is to **encode the label into the group name**, using virtual subscription groups such as `G%label`. Offset, checkpoint, revive, and retry can all be reused automatically, with no new offset table |
| **The POP path does not support LMQ at all**. `PopMessageProcessor` contains zero `isLmq` references, and LMQ serves only PULL through `LmqPullRequestHoldService` | `PopMessageProcessor.java`; `broker/.../longpolling/LmqPullRequestHoldService.java` | **This invalidates the earlier LMQ-based candidate**. Supporting LMQ would require changing the entire POP path, which is far more expensive than using virtual groups |
| The per-message deliver-or-skip decision point is `messageFilter`, and skipped messages still advance the cursor through `nextBeginOffset` | `PopMessageProcessor.java:774-776`, `appendCheckPoint:831` | Label-based deliver / skip logic can be attached to the filter, **without changing cursor advancement logic** |

> The LMQ option is officially abandoned for this plan and will not be considered further.

## 1. Carrier: Virtual Subscription Group Equals Sub-Cursor

Instead of inventing a new offset table, each traffic label is encoded into a **virtual subscription group**, reusing existing `topic@group` accounting.

| Role | Real / virtual group | Message filter | Who performs POP |
|---|---|---|---|
| Standard consumer | Real group `G` | `label == STANDARD` | Standard consumer |
| gray1 online | Virtual group `G%gray1` | `label == gray1` | gray1 consumer |
| gray1 offline | Virtual group `G%gray1` | `label == gray1` | **Standard consumer for harvesting** |

**Core rule: the standard group `G` always consumes only `STANDARD`. Every message for grayX always belongs to virtual group `G%grayX`. When grayX is online, it POPs `G%grayX` itself. When grayX is offline, standard consumers take over `G%grayX`.**

Compared with dynamically allowing offline labels through the standard filter, this is a much more converged design. The filter on standard group `G` stays fixed and is not affected by online-snapshot flapping. Snapshot errors only affect whether standard should go harvest a virtual group. At worst that causes delay or duplication, with at-least-once semantics and consumer idempotency as the safety net. **It does not lose messages.**

## 2. Ownership Transfer, Not Message Transfer

The earlier "counterexample B gets stuck" does not exist at all under the virtual-group model:

```text
queue: [stdA, gray1B, stdC]

gray1 online:
  gray1 POPs G%gray1 with filter=gray1 -> reads B, and B is accounted on cursor G%gray1
  standard POPs G with filter=STANDARD -> reads stdA/stdC, while B does not belong to G

gray1 offline:
  harvesting round by standard POPs G%gray1 with filter=gray1
    case 1: gray1 read B but crashed before ack
            -> B is in the checkpoint of G%gray1
            -> revive redelivers it to %RETRY%G%gray1
            -> standard harvesting then receives it from retry
    case 2: gray1 had not reached B yet
            -> standard continues from the G%gray1 cursor and reads B

  In both cases, POP normal flow + retry + revive are reused 100 percent, with zero special handling
```

The key is that **B never belongs to standard group `G`**. B is always accounted on `G%gray1`. Ownership of the virtual group moves between gray1 and standard, but the message itself does not move.

## 3. Change List with Anchors

1. **`PopMessageRequestHeader`**: add `consumerLabel` plus `onlineLabelsSnapshot`, or a snapshot version with broker caching.
2. **Proxy**: maintain the online-label snapshot through `ClusterConsumerManager` plus `HeartbeatSyncer`, and route as follows:
   - gray consumer -> mapped to `G%label`
   - standard consumer -> real group `G` plus the offline-label list used to drive harvesting
3. **`PopMessageProcessor.processRequest`**: virtual groups such as `G%grayX` are rejected at `:308 findSubscriptionGroupConfig` because the group does not exist. This is a **mandatory change point**: virtual groups must inherit the parent group `G`'s `SubscriptionGroupConfig`.
4. **Message-filter construction** at `:326-356`: inject label-based property filtering using `__RMQ_TRAFFIC_LABEL`.
5. **Harvest scheduling**: the broker maintains a harvest queue, and each standard POP response piggybacks one harvest group in round-robin order, using the full `popMsgFromTopic` path for both origin and retry.
6. **Grace period**: add `gracePeriodMs`. A label enters the harvest queue only after remaining offline past that threshold. The grace period is the unified knob between takeover and fallback.
7. **Virtual-group cleanup**: only after the **three-condition check** is satisfied, meaning origin offset is drained, retry offset is drained, and revive has no in-flight checkpoint for the group, plus a re-check of the online snapshot in the cleanup critical section, should the system reclaim the three offset locations for `G%grayX`, the subscription compensation state, and the retry-topic config.
8. **Concurrency**: `popMsgFromQueue` already uses lock key `topic#group#queueId` at `:695`, so virtual groups are naturally isolated. The race between gray1 being judged offline and suddenly coming back is handled by that lock, the snapshot, the grace period, and consumer-side idempotency.
9. **Master/slave switchover integration**: the harvesting service and grace-period timers should hook into `BrokerController.java:2402 changeSpecialServiceStatus`, run only on master, stop and clear on slave, and rebuild cold when becoming master again.
10. **Persistent shared cursor for takeover semantics**: cursor `G%label` persists across generations, **without epoch**. A rebuilt gray reuses the existing cursor instead of triggering `getInitOffset`.

## 4. Locked Decisions (Confirmed on 2026-06-29)

> Locked premise: isolated environments are **temporary**, such as PR preview or load-test environments that are created and destroyed frequently, and **fallback to standard after gray goes offline is required**.
> Immediate consequence: the static Plan-B-Lite variant described later is **not sufficient**. The full harvesting version is required.

### Decision A: Harvesting Is Driven by Broker Fan-In plus Round-Robin Piggyback

The naive strategy of having standard POP fan in all offline groups would expand from 1 to 1+N under temporary environments with many flapping groups. The refined decision is:

- The broker maintains a **harvest queue**. Entry conditions are described in section 4a.
- Standard consumers performing ordinary POP read only real group `G`, with **zero amplification on the normal path**.
- In each standard POP response, the broker **piggybacks one harvest group** such as `G%grayX` in round-robin order, keeping amplification bounded at **1+1**.
- Once harvesting and cleanup complete, the group is removed from the harvest queue.

### Decision B: The Lifecycle Is Temporary, So Harvesting Is Mandatory

See sections 4a and 4b.

## 4a. Temporary Environments: Offline Does Not Mean Destroyed

In temporary environments, a gray environment disappearing can mean one of two things, and the online snapshot **cannot distinguish them**:

| Type | Meaning | Correct handling |
|---|---|---|
| Flap / restart | Goes offline briefly, then comes back | **Do not harvest immediately**, or standard and the returning gray will race and create duplicates |
| Destroyed | Gone permanently | Harvest cleanly, then reclaim the virtual group |

The solution is **grace period `gracePeriodMs`**. A gray label enters the harvest queue only after remaining offline for T. T is the **single knob between takeover and fallback**:

- If gray returns within the grace period, it resumes from the persistent cursor `G%grayX` and **fully takes over the backlog**.
- Once the grace period expires, standard harvesting begins and fallback starts.
- If T is too small, a normal rebuild may not finish in time, and standard will harvest too early. Gray can only take over the remainder.
- If T is too large, fallback latency after a true destroy becomes too high, and backlog stays longer on `G%grayX`.

If gray comes back within the grace period, it is removed from the harvest queue immediately and continues from the existing cursor with no side effect.

## 4b. Virtual-Group Cleanup Is Mandatory for Temporary Environments

Without cleanup, repeated create-destroy cycles make metadata for `G%grayX` grow without bound.

Warning: **cleanup cannot look only at the origin offset**. Retry-topic state plus asynchronous revive can create orphan messages. The correct cleanup rule is that all three conditions in section 4c must be satisfied together.

```text
gray destroyed
  -> offline duration exceeds gracePeriodMs
  -> enters harvest queue
  -> standard piggybacks full POP on G%grayX, including origin + retry
  -> all three conditions become true
  -> cleanup critical section: lock (G, grayX) and re-check the online snapshot
       if grayX is back online:
         cancel cleanup and keep the cursor for takeover
       else:
         reclaim the three offset locations of G%grayX, subscription compensation state, and retry-topic config
```

Cleanup targets only gray environments that are **truly destroyed**. If gray returns within the grace period, the cursor is taken over instead and cleanup does not fire.

## 4c. Handling Unconsumed Messages on the Retry Topic

If gray1 fails to consume a message, that message is no longer on the origin topic. It goes to the **retry topic owned by the virtual group**, `%RETRY%G%gray1`. Three source-level facts determine the handling:

| Fact | Anchor | Meaning |
|---|---|---|
| Retry topics are named by consumer group | `KeyBuilder.java:32-41` | Failed messages of `G%gray1` go into `%RETRY%G%gray1`, completely isolated from standard retry |
| One POP naturally pulls origin and retry together | `PopMessageProcessor.java:532-561` | If harvesting uses the **full virtual-group POP**, retry messages are harvested automatically, with no separate retry logic |
| Revive is **asynchronous delayed redelivery** | `PopReviveService.java:113-159` | Right after origin is drained, failed messages may still be waiting inside revive and have not been re-put into retry yet. Cleaning up at that moment would create orphans |

### Constraint 1: Harvesting Must Use Full POP

Harvesting `G%grayX` must reuse the existing `popMsgFromTopic` path that pulls **both origin and retry**. It must **not** be optimized into scanning only the origin consume queue, or retry messages will be missed forever. Reuse is simpler: retry is covered automatically.

### Constraint 2: Cleanup Must Use Three Conditions

The old rule of "origin offset == maxOffset" is **wrong**. Counterexample: gray1 crashes just after a message B enters checkpoint and before revive re-puts it into retry. Origin is already drained, retry is still empty, and cleanup would look safe. A few seconds later, revive re-puts B into `%RETRY%G%gray1`, but the virtual group has already been reclaimed and no one POPs it anymore, turning B into a permanent orphan.

The correct cleanup rule is that all three conditions must hold at the same time:

```text
1) origin G%gray1        : offset == maxOffset
2) retry  %RETRY%G%gray1 : offset == maxOffset
3) revive has no in-flight checkpoint for G%gray1
```

Condition 3 is the key defense. Cleanup must wait until revive has completely drained the group's pending checkpoints, either by re-putting them to retry or finalizing them. At cleanup time, remove `G%gray1@origin`, the offsets for `%RETRY%G%gray1`, and any related checkpoint residue.

## 4d. Broker Master/Slave Switching and Restart

The harvest queue and grace-period timers are **in-memory derived state**. The conclusion is to follow RocketMQ's existing pattern: **persist nothing new, clear state on role change, and rebuild from persistent truth after restart or switchover**.

### Existing Pattern from Source

| Fact | Anchor | Meaning |
|---|---|---|
| Revive progress is **persisted** as offset in a system group and replicated with master/slave sync | `PopReviveService.java:84` | On restart or failover, in-flight failed messages do not lose revive progress and can continue being re-put to `%RETRY%G%grayX` |
| POP in-memory buffering is designed to be **discardable** | `PopBufferMergeService.java:74-101` | In-memory buffers are acceleration only. Persistent offset and checkpoint stores hold the truth |
| There is a unified special-service switchover hook | `BrokerController.java:2402 changeSpecialServiceStatus` | This is the right place to mount the harvesting service |
| The consumer online table itself is non-persistent and rebuilt by heartbeat | `ConsumerManager.java:44`, `scanNotActiveChannel` | The online-label snapshot can also rebuild naturally without persistence |

### Design Principle: Derived State Has Zero Persistence

The harvest queue and grace-period timers are not the source of truth. They are only a to-do index. Persistent truth is elsewhere.

| In-memory state | After restart / switchover | Source of truth |
|---|---|---|
| Harvest queue | Discard and infer again on cold start | Existence of virtual-group offset plus absence of online consumer |
| Grace-period timer | Restart from zero | Online label state rebuilt from heartbeat, at worst delaying by one extra grace period |
| Harvest progress | No restoration required | Offset for `G%grayX` is already persistent through `topic@group` |
| Revive of failed messages | Continues automatically | reviveOffset is persisted and replicated |

### Three Rules

- **Rule 1: role awareness**. The harvesting service and grace-period timers run only on master, hook into `changeSpecialServiceStatus`, and stop and clear immediately when the broker becomes slave.
- **Rule 2: cold-start rebuild**. When becoming master again, scan all `G%label` virtual-group offsets, compare them against the current online snapshot, and restart grace-period timing for offline labels from zero. At worst, harvesting is delayed by one extra grace period, but messages are not lost.
- **Rule 3: crash during harvesting**. If a harvested message was POPed but not acked before crash, its checkpoint is already persisted. The new master's revive process re-puts it to `%RETRY%G%grayX`, and the next harvest round picks it back up. This is naturally compatible with the three-condition cleanup rule.

## 4e. Same-Name Rebuild Takes Over the Previous Generation's Backlog

> User decision: if gray is rebuilt with the same name, it **takes over the previous generation's backlog** instead of letting everything fall back to standard.
> This directly conflicts with immediate fallback after gray goes offline. The grace period is the only way to reconcile the two.

### Core Tension

For a message M that arrives while gray1 is offline:

- **Takeover semantics** means M must be kept for gray1 to return and consume.
- **Fallback semantics** means M must be given to standard immediately.

Both cannot be true for the same message. Choosing takeover semantics means accepting that **fallback is delayed**.

### Mechanism: Remove Epoch and Share the Cursor by Label

The earlier epoch-based scheme, such as `G%gray1%e2`, physically isolated generations and therefore **prevented takeover**. To enable takeover, epoch must be removed. The cursor becomes `G%gray1`, persistent across generations.

```text
gray1 offline
  -> within the grace period:
       standard does not harvest, backlog waits for gray1
       if gray1 returns:
         it resumes from G%gray1 and gets 100 percent of the backlog
       else if the grace period expires:
         standard begins harvesting G%gray1
         if gray1 returns later:
           it resumes from G%gray1 and gets only the part standard has not already harvested
```

- **Fallback delay equals grace-period length**. Choose the grace period around the typical rebuild time. Fast rebuilds get full takeover. Truly dead gray environments eventually fall back after that delay.
- On its first POP after rebuild, gray1 sees that cursor `G%gray1` still exists, so offset is not less than zero. That means `getInitOffset` is not triggered, and gray1 resumes from backlog instead of jumping to `max-1`.

### New Defense for the Old "Cleanup Deletes Progress" Failure

Without epoch, physical separation can no longer protect against deleting the active successor's progress. Instead, cleanup must perform an **atomic re-check**:

```text
before cleanup, lock (G, gray1) and re-check the online snapshot
  if gray1 is online:
    cancel cleanup and let gray1 take over
  else if the three conditions from section 4c still hold:
    delete G%gray1
```

The remaining sub-millisecond race window is handled by at-least-once semantics plus consumer idempotency, which the system already requires.

### Cleanup Is Still Required, Just Later

Takeover semantics does not eliminate cleanup. Truly destroyed gray environments still need metadata reclamation, or state grows forever. The only change is timing: cleanup happens only after the grace period has expired, standard harvesting has fully drained the backlog, and the re-check still says gray is offline. If gray comes back earlier, it takes over and cleanup never triggers.

## 5. Pros and Cons

**Pros**
- **No message-body copying**. There are no route topics, bypass stores, or route indexes.
- **No revive special handling**. Failed-message retry reuses the POP retry and revive flow of each virtual group.
- **Reuses existing offset accounting**. Virtual groups map directly onto `topic@group`.
- **Zero amplification on the normal path**. Standard POP reads only `G`, and harvesting uses round-robin piggybacking so each response grows by at most 1+1.

**Cons / Risks**
- **Virtual-group metadata growth**. Offset records grow with label count times queue count. In temporary environments this is controlled by cleanup, but when label count becomes very large, plan A may be preferable.
- **Grace-period tuning**. Too small means flaps are mistaken for death and duplicates increase. Too large means fallback latency gets worse.
- **Harvesting concurrency races**. A returning gray can race with standard harvesting. This requires locking, grace-period control, and idempotency.
- **Fallback is not instantaneous**. It depends on grace period plus harvesting rotation.
- **Heavy reliance on the online snapshot**. Standard must know which labels exist and are offline in order to harvest correctly.
- **Cold-start delay after master/slave switching**. When becoming master again, the grace period restarts from zero, so harvesting may be delayed by one extra `gracePeriodMs`, though messages are not lost.

## 6. Simplified Variant for Permanent Isolated Environments

> This project does **not** use this variant. It is kept only for comparison.

If isolated environments are **long-lived and only flap occasionally**, the design can be simplified into **pure virtual groups with no harvesting**. When gray1 returns after a flap, it simply continues POPing `G%gray1`. That adds almost no extra machinery. Temporary environments cannot use that simplification, because destroyed gray environments never come back and their backlog would remain forever.

## 7. Suitable Scenarios

- **This project, temporary environments with required fallback, uses the full harvesting version** in sections 4, 4a, and 4b
- Permanent isolated environments with occasional gray deployment can use the simplified virtual-group variant
- Extremely large label counts can make virtual-group growth too expensive, which pushes the choice back toward [[2026-06-29-traffic-label-routing-plan-a-strict-bypass]]

## 8. Extension-Point Feasibility Analysis

Split plan-b into five required capabilities, then compare each against the extension points documented in [[Server_Extension_Points]](`docs/cn/Server_Extension_Points.md`):

| Required capability | Can existing extension points carry it? | Anchor | Conclusion |
|---|---|---|---|
| 1) Online label snapshot as a cluster view | No. `ClusterConsumerManager` and `HeartbeatSyncer` are core components and would need label dimension added | - | Core change required |
| 2) Rewrite POP routing from `group` to `G%label` | Proxy `RequestPipeline` can intercept, but 1) it is not SPI and 2) it is one-to-one only and cannot do fan-in | `proxy/.../grpc/pipeline/RequestPipeline.java` | Core change required |
| 3) Make virtual groups POP-able | No extension point can forge a subscription group | `PopMessageProcessor.java:308` | Core change required, or pre-create real groups operationally |
| 4) Filter / SKIP by label | Proxy `PopMessageResultFilter.NO_MATCH` **acks and loses the message**, so it cannot implement fallback. Store `MessageFilter` can SKIP, but its injection point is not SPI. **SQL92 `ExpressionMessageFilter` already works on the POP path** | `ConsumerProcessor.java:181-190`, `PopMessageProcessor.java:326` | SQL92 gives zero broker intrusion for this capability |
| 5) Harvest fan-in so standard reads `G%grayX` | No extension point. `ConsumerProcessor.popMessage` is strictly one-to-one | `ConsumerProcessor.java:79` | Core change required, or an external operator |

> Trap: Proxy `PopMessageResultFilter` looks like it might support "drop by label", but the `NO_MATCH` branch directly calls `ackMessage`. The message is confirmed and disappears permanently, so it cannot support fallback.

**Conclusion: a completely zero-intrusion path based only on extension points, configuration, and operational setup can provide only static isolation, not dynamic fallback.** Dynamic fallback depends on 1) the online snapshot and 5) harvesting fan-in, and no extension point covers either.

## 10. Component Boundaries and Data Flow

### 10.1 Component Boundary Diagram

```text
+------------------------------------------------------------------+
|                            Proxy                                 |
|                                                                  |
|  +--------------------------+  +------------------------------+  |
|  | LabelSnapshotManager     |  | POPRouter                    |  |
|  | consumerTable            |  | gray consumer -> G%label     |  |
|  | Map<label, Set<instance>>|->| standard consumer -> G       |  |
|  | heartbeat timeout marks  |  | plus offlineLabels[]         |  |
|  | label offline and starts |  +---------------+--------------+  |
|  | GracePeriodTimer                         | POP request header |
|  +--------------------------+               | gRPC ReceiveMessage|
+-----------------------------+---------------+--------------------+
                              |               v
+------------------------------------------------------------------+
|                            Broker                                |
|                                                                  |
|  +---------------------+   +--------------------+                |
|  | VirtualGroup        |   | LabelMsgFilter     |                |
|  | Compensator         |   | G%grayX: label==x  |                |
|  | G%grayX inherits G  |   | G(std): STANDARD   |                |
|  +----------+----------+   | or IS NULL         |                |
|             |              +---------+----------+                |
|             v                        v                           |
|  +----------------------------------------------------------+   |
|  | PopMessageProcessor                                      |   |
|  | popMsgFromTopic(origin + retry together)                 |   |
|  | lockKey = topic#group#queueId                            |<-+ HarvestScheduler
|  | offset -> ConsumerOffsetManager(topic@group)             |   | round-robin piggyback
|  +----------------------------------------------------------+   |
|                                                                  |
|  +----------------------+  +----------------------------------+  |
|  | GracePeriodTimer     |  | VirtualGroupLifecycleManager     |  |
|  | label offline -> T   |  | three-condition check            |  |
|  | timeout -> enqueue   |  | cleanup critical section         |  |
|  | gray returns -> stop |  | lock(G, grayX) re-check snapshot |  |
|  | mounted on special   |  | delete offset/config             |  |
|  | service status hook  |  +----------------------------------+  |
|  +----------------------+                                       |
|                                                                  |
|  +----------------------+                                       |
|  | HarvestScheduler     |                                       |
|  | harvestQueue         |                                       |
|  | round-robin pick one |                                       |
|  | piggyback into std   |                                       |
|  | POP response, max 1+1|                                       |
|  +----------------------+                                       |
+------------------------------------------------------------------+
```

**Interface contracts**

| Boundary | Interface | Data |
|---|---|---|
| Proxy -> Broker POP header | `PopMessageRequestHeader` | `consumerLabel`, `onlineLabelsSnapshot` as full snapshot or version |
| Broker -> Proxy POP response | `PopMessageResponse` | Standard messages plus piggybacked harvest messages in one unified `MessageExt` list |
| LabelSnapshotManager -> GracePeriodTimer | Event signal | `(label, offline/online, timestamp)` |
| GracePeriodTimer -> HarvestScheduler | Enqueue signal | `label` |
| HarvestScheduler -> PopMessageProcessor | Harvest parameters | `group=G%grayX, topic` |
| VirtualGroupLifecycleManager -> ConsumerOffsetManager | Delete operation | `topic@G%grayX`, `topic@%RETRY%G%grayX` |

### 10.2 Three Core Data Flows

#### Flow 1: gray1 Consumes Normally While Online

```text
gray1 consumer --POP G--> Proxy POPRouter
  -> rewrites G to G%gray1, sets consumerLabel=gray1

Broker PopMessageProcessor(group=G%gray1)
  -> VirtualGroupCompensator inherits config from G
  -> LabelMsgFilter uses label==gray1
  -> popMsgFromTopic(G%gray1, origin + retry)
  -> ConsumerOffsetManager advances cursor G%gray1@topic

Return only label==gray1 messages to the gray1 consumer
```

#### Flow 2: gray1 Goes Offline and Standard Harvests It

```text
gray1 offline -> LabelSnapshotManager notices
  -> GracePeriodTimer starts timing gracePeriodMs
     if gray1 returns within grace period:
       cancel timer and go back to Flow 1
     else:
       HarvestScheduler.enqueue(gray1)

std consumer --POP G--> Proxy POPRouter
  -> sends group=G with offlineLabels=[gray1]

Broker PopMessageProcessor(group=G)
  -> LabelMsgFilter keeps only STANDARD or null
  -> HarvestScheduler round-robin picks G%gray1
  -> full popMsgFromTopic(G%gray1, origin + retry)
  -> ConsumerOffsetManager advances G%gray1@topic

POP response = standard messages + piggybacked harvest messages
Standard consumer processes both batches and ACKs them separately

Once harvesting continues until the three conditions hold:
  -> VirtualGroupLifecycleManager locks (G, gray1)
  -> re-checks online snapshot
     if gray1 came back:
       cancel cleanup
     else:
       delete G%gray1 offsets + retry config

gray1 is removed from the harvest queue and cleanup is complete
```

#### Flow 3: Same-Name gray1 Rebuild Takes Over Backlog

```text
gray1 rebuilds -> heartbeat reaches Proxy
  -> LabelSnapshotManager marks gray1 online
  -> HarvestScheduler removes G%gray1 if present
  -> GracePeriodTimer cancels if running

gray1 --POP G--> Proxy POPRouter -> G%gray1

Broker PopMessageProcessor(group=G%gray1)
  -> ConsumerOffsetManager.queryOffset(topic@G%gray1)
  -> offset exists and is persistent across generations
  -> offset != -1, so getInitOffset is not called
  -> consumption resumes from the previous position

gray1 continues from backlog
Any part already harvested by standard is not returned again
Standard sees gray1 online and stops piggyback harvesting of G%gray1
```

## 11. E2E and API Test Cases

> Per planning rules, every core user flow should have at least one E2E case. Tests should not mock dependent services. Use a real Broker + Proxy setup through Testcontainers or local deployment.

### 11.1 Core Flow Matrix

| Test ID | Flow type | Core assertion |
|---|---|---|
| E2E-01 | Fallback flow: gray goes offline and standard takes over | No message loss. Standard consumes gray messages after grace period ends |
| E2E-02 | Takeover flow: same-name gray rebuild inherits backlog | Rebuilt gray resumes from the previous cursor and standard does not consume those gray messages |
| E2E-03 | Destroy-and-cleanup flow | All messages are consumed, and `G%gray1` offset records are reclaimed |
| E2E-04 | Isolation with multiple gray environments | gray1 messages go only to gray1, gray2 messages go only to gray2 |
| E2E-05 | Flap protection | Short offline windows do not trigger harvesting |

### 11.2 E2E-01: Fallback Flow

```text
Prerequisites:
  - Broker + Proxy running, topic=test-topic, queueNum=4
  - standard consumer group=G, SQL92: label IS NULL OR label='STANDARD'
  - gray1 consumer group=G%gray1, SQL92: label='gray1'
  - gracePeriodMs=10s

Steps:
  1. Bring gray1 online and establish heartbeat
  2. Send 20 messages with label=gray1, M1-M20
  3. Assert that gray1 consumes M1-M20 and standard consumes none
  4. Stop gray1 to simulate offline
  5. Wait 5s, which is inside the grace period, then send 10 more gray1 messages M21-M30
  6. Assert that standard still consumes none of M21-M30 during this 5s window
  7. Wait until gracePeriodMs + 5s
  8. Send another 10 gray1 messages M31-M40
  9. Assert that standard consumes M21-M40, including backlog accumulated during grace period plus new messages

Detailed assertions:
  - gray1 consumed count = 20
  - standard consumed count = 20
  - No duplicates, verified through msgId
  - No message loss: produced total 40 = consumed total 40
```

### 11.3 E2E-02: Takeover Flow for Same-Name Rebuild

```text
Prerequisite: gracePeriodMs=15s

Steps:
  1. Bring gray1 online and let it consume M1-M10
  2. Stop gray1 with the cursor at M10
  3. Immediately send 20 more gray1 messages M11-M30
  4. Wait 8s, which is still inside the 15s grace period
  5. Assert that standard consumes none of M11-M30 during the grace period
  6. Rebuild gray1 using the same group G%gray1
  7. Wait until gray1 finishes consumption
  8. Assert that gray1 resumes from M11 rather than replaying from M1
  9. Assert that standard consumes none of M11-M30
  10. Assert that produced total 30 = gray1 consumed total 30

Key verification:
  - The rebuilt gray1 starts after M10 because the shared cursor was preserved
  - Standard consumption count for M11-M30 = 0
```

### 11.4 E2E-03: Destroy-and-Cleanup Flow

```text
Prerequisites: gracePeriodMs=10s, cleanup check interval=5s

Steps:
  1. Bring gray1 online and send 30 messages with label=gray1
  2. Let gray1 consume 15 successfully, M1-M15, fail 5, M16-M20, into retry, and leave M21-M30 unconsumed
  3. Stop gray1 and do not rebuild it
  4. Wait for gracePeriodMs so that standard harvesting starts
  5. Wait until harvesting completes, including retry messages M16-M20
  6. Wait until revive finishes
  7. Wait until the three conditions hold and cleanup triggers

Detailed assertions:
  - Standard consumed count = 15, covering M16-M30 including retry
  - Total consumed = 30 = gray1 consumed 15 + standard consumed 15
  - After cleanup, `test-topic@G%gray1` does not exist in ConsumerOffsetManager
  - After cleanup, `test-topic@%RETRY%G%gray1` does not exist
  - No orphan messages: retry topic `%RETRY%G%gray1` has offset == maxOffset
```

### 11.5 E2E-04: Isolation with Multiple Gray Environments

```text
Steps:
  1. Bring all three online: gray1 consumer G%gray1, gray2 consumer G%gray2, and standard consumer G
  2. Send 10 messages for each of the three buckets: gray1, gray2, and STANDARD

Assertions:
  - gray1 consumes and only consumes the 10 gray1 messages
  - gray2 consumes and only consumes the 10 gray2 messages
  - standard consumes and only consumes the 10 STANDARD messages
  - No cross-environment contamination occurs
```

### 11.6 E2E-05: Flap Protection

```text
Prerequisite: gracePeriodMs=20s

Steps:
  1. While gray1 is online, send 20 messages with label=gray1
  2. Stop gray1 to simulate a GC or restart flap
  3. Wait 10s, which is below the grace period, then bring gray1 back online
  4. Wait for gray1 to finish consuming

Assertions:
  - Standard consumes zero gray1 messages because harvesting never starts inside the grace period
  - gray1 consumes all 20 messages and fully takes over
  - gray1 either never appears in the harvest queue, or appears only transiently and is removed before harvesting
```

### 11.7 API Tests: POP Request and Response Contract

| Case | Request | Expected response |
|---|---|---|
| Normal gray POP | `consumerLabel=gray1`, `onlineLabelsSnapshot={gray1:online}` | Returns only `label=gray1` messages |
| Normal standard POP | `consumerLabel=STANDARD`, `offlineLabels=[]` | Returns only `label=STANDARD OR IS NULL` messages |
| Standard POP while harvesting | `consumerLabel=STANDARD`, `offlineLabels=[gray1]` | Returns standard messages plus piggybacked messages from `G%gray1`, bounded to 1+1 |
| Virtual-group POP without config | `group=G%gray1`, config absent | Inherits config from G and succeeds rather than returning `GROUP_NOT_EXIST` |
| Snapshot version hit | `snapshotVersion=N`, matching broker cache | Reuses broker-side snapshot cache without sending the full snapshot |

## 9. Minimal-Intrusion Variant: Plan-B-Lite

SQL92 support from capability 4 makes a **static isolation** variant possible with zero broker code changes:

1. Pre-create a real subscription group `G%gray1` for each isolated environment through mqadmin.
2. gray consumers use `group=G%gray1` with SQL92 subscription `__RMQ_TRAFFIC_LABEL = 'gray1'`.
3. standard consumers use `group=G` with SQL92 subscription `__RMQ_TRAFFIC_LABEL IS NULL OR __RMQ_TRAFFIC_LABEL = 'STANDARD'`.
4. Enable `enablePropertyFilter` on the broker.

Different groups mean independent cursors, which resolves the single-cursor tension from [[2026-06-29-traffic-label-routing-design]] section 2. SQL92 provides the label-level filtering that already exists today. **No broker or Proxy code needs to change at all.**

### Gap: Dynamic Fallback

Plan-B-Lite lacks exactly the fallback behavior. Once gray1 goes offline, backlog on `G%gray1` remains there and standard consumers subscribed only to STANDARD will never touch it. There are only two ways to add fallback:

| Option | Mechanism | Broker intrusion | Cost |
|---|---|---|---|
| **A. Change core and add harvesting** | Return to the full plan-b approach with virtual groups plus standard fan-in harvesting | Yes, but concentrated in Proxy routing and the virtual-group compensation point at `PopMessageProcessor:308` | Controlled change surface with real-time fallback |
| **B. Use an external operator** | Monitor gray online state and dynamically have standard instances join `G%gray1` for harvesting while gray is offline, then remove them once gray returns | **Zero** | Requires an extra external component, slower fallback, and introduces online/offline races |

### Recommendation

- If isolated environments are **long-lived**, the static Plan-B-Lite variant is almost enough and truly zero intrusion.
- If you **must support dynamic fallback**, changing core code for harvesting is more reliable than introducing an external operator, and the intrusion is smaller than it first appears.
