# RocketMQ POP Consume & DLQ Flow

## Overview

RocketMQ has three consumer paths. Each handles retry and DLQ differently.

| Path | Transport | DLQ Mechanism |
|---|---|---|
| Classic Push (`ConsumeMessageConcurrentlyService`) | Remoting | Broker decides at `CONSUMER_SEND_MSG_BACK` |
| POP Push (`ConsumeMessagePopConcurrentlyService`) | Remoting + POP | Client drops silently after 4h, **no DLQ** |
| gRPC PushConsumer | gRPC + Proxy | Proxy filters to DLQ before message reaches client |

---

## Path 1: Classic Push Consumer — NACK → DLQ

### Client side

```
ConsumeMessageConcurrentlyService.processConsumeResult()
  │
  ├─ CONSUME_SUCCESS  → ackIndex = last success index
  └─ RECONSUME_LATER  → ackIndex = -1  (all messages fail)

  for i = ackIndex+1 to msgs.size():           // all failed messages
    sendMessageBack(msg, context)
      └─ DefaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, brokerName)
           └─ MQClientAPIImpl.consumerSendMessageBack(
                  brokerAddr, brokerName, msg,
                  consumerGroup,
                  delayLevel,             // default 0
                  5000,                   // timeout ms
                  getMaxReconsumeTimes()  // default 16
              )
              → CONSUMER_SEND_MSG_BACK request to broker

  [if sendMessageBack() throws]:
    msg.setReconsumeTimes(msg.getReconsumeTimes() + 1)
    submitConsumeRequestLater(failed)   // local re-submit, no broker
```

### Broker side (`AbstractSendMessageProcessor.consumerSendMsgBack`)

```
SendMessageProcessor.processRequest()
  └─ case CONSUMER_SEND_MSG_BACK:
       AbstractSendMessageProcessor.consumerSendMsgBack()
         │
         ├─ peekMasterBroker()          // must be master; fail if none
         ├─ findSubscriptionGroupConfig(group)
         │     maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes()
         │     [overridden by requestHeader.maxReconsumeTimes if version >= V3_4_9]
         │
         ├─ lookMessageByOffset(offset) // read original msg from CommitLog
         │
         ├─ resolve delayLevel:
         │     default: 0 (client default)
         │
         ├─── DLQ branch ──────────────────────────────────────────────────────
         │  if msgExt.getReconsumeTimes() >= maxReconsumeTimes  // 16 by default
         │     OR delayLevel < 0:                               // client forces DLQ
         │
         │     isDLQ = true
         │     newTopic  = "%DLQ%group"
         │     queueId   = random(DLQ_NUMS_PER_GROUP=1)
         │     delayTimeLevel = 0    // no delay for DLQ
         │     createTopicInSendMessageBackMethod(newTopic, 1, R|W, 0)
         │─────────────────────────────────────────────────────────────────────
         │
         └─── Retry branch ────────────────────────────────────────────────────
            else:
              if delayLevel == 0:
                delayLevel = 3 + msgExt.getReconsumeTimes()
                // times=0 → level 3 → 10s
                // times=1 → level 4 → 30s
                // times=2 → level 5 → 1min
                // ...up to level 18 → 2h
              newTopic = "%RETRY%group"
              → SCHEDULE_TOPIC_XXXX (delay queue) → %RETRY%group after delay
            ───────────────────────────────────────────────────────────────────

         build MessageExtBrokerInner:
           msgInner.setTopic(newTopic)                    // DLQ or retry topic
           msgInner.setReconsumeTimes(times + 1)
           msgInner.setDelayTimeLevel(...)

         masterBroker.getMessageStore().putMessage(msgInner)
         [if isDLQ]: BrokerStatsManager.incDLQStatValue()
                     DLQ_LOG.info("send msg to DLQ %DLQ%group ...")
```

**DLQ trigger**: `reconsumeTimes >= maxReconsumeTimes` (default: 16th failure = 17th attempt)

---

## Path 2: POP Push Consumer (`ConsumeMessagePopConcurrentlyService`) — No DLQ

```
ConsumeMessagePopConcurrentlyService.processConsumeResult()
  │
  ├─ CONSUME_SUCCESS  → ackAsync(msg) for each success
  └─ RECONSUME_LATER  → ackIndex = -1

  for each failed msg:
    ├─ if reconsumeTimes >= maxReconsumeTimes (16):
    │    checkNeedAckOrDelay(msgExt)
    │      │
    │      │  msgDelaytime = now - msg.bornTimestamp
    │      │  popDelayLevel = [10,30,60,120,180,240,300,360,420,480,540,600,1200,1800,3600,7200]s
    │      │
    │      ├─ if msgDelaytime > 7200 * 2s (4h total):
    │      │    ackAsync(msg)    ← ACK = drop silently, NO DLQ
    │      │
    │      └─ else:
    │           delayLevel = first level where msgDelaytime >= level * 1000
    │           changePopInvisibleTimeAsync(delayLevel * 1000)
    │           ← extend invisible time, broker re-delivers after delay
    │
    └─ else (reconsumeTimes < 16):
         changePopInvisibleTime(msg, delayLevel)
         // delayLevel = reconsumeTimes (grows with each retry)
         // broker re-delivers after delayLevel seconds
```

**Key**: When `reconsumeTimes >= 16`, the message is **not sent to DLQ**. It is kept alive via `changeInvisibleTime` until it has been in-flight for more than 4 hours total, then silently ACKed (dropped).

---

## Path 3: gRPC PushConsumer (v5 SDK via Proxy)

The v5 SDK is in `apache/rocketmq-clients` (separate repo). DLQ routing differs by message type:

- **Standard (concurrent) messages**: DLQ decided at proxy, client only does NACK via `changeInvisibleDuration`
- **FIFO (ordered) messages**: client retries locally, then explicitly DLQs — no proxy filtering for FIFO

### v5 Client-side flow (StandardConsumeService + ProcessQueueImpl)

```
StandardConsumeService.consume(pq, messages):
  └─ for each messageView:
       if isCorrupted() → pq.discardMessage(mv) → nackMessage()   // changeInvisibleDuration forever

       ListenableFuture<ConsumeResult> future = consume(messageView)
         └─ MessageListener.consume(messageView, ctx)   // user code
              returns ConsumeResult.SUCCESS or FAILURE

       pq.eraseMessage(messageView, consumeResult):
         ├─ SUCCESS → ackMessage(mv)
         │    → AckMessageRequest → proxy → broker ACK fast/slow path
         │
         └─ FAILURE → nackMessage(mv)
              deliveryAttempt = messageView.getDeliveryAttempt()
              duration = retryPolicy.getNextAttemptDelay(deliveryAttempt)
                └─ ExponentialBackoffRetryPolicy:
                     delay = min(initialBackoff * multiplier^(attempt-1), maxBackoff)
              changeInvisibleDuration(mv, duration)
                → ChangeInvisibleDurationRequest → proxy → ChangeInvisibleTimeProcessor
                   (writes new CK + ACKs old CK, does NOT increment reconsumeTimes)
```

**Key**: Standard consumer NEVER calls `ForwardMessageToDeadLetterQueueRequest`.
The client unconditionally NACKs (changeInvisibleDuration) for every failure.
DLQ is enforced by the proxy at the NEXT receive (see below).

### v5 Client-side FIFO flow (FifoConsumeService + ProcessQueueImpl)

```
FifoConsumeService.consume(pq, messages):
  └─ consumeIteratively(pq, iterator):
       messageView = nextValidMessage() [skip corrupted → discardFifoMessage → forwardToDLQ]

       future = consume(messageView)   // MessageListener returns SUCCESS or FAILURE
       future → pq.eraseFifoMessage(messageView, result):

         if FAILURE && attempt < maxAttempts:
           nextAttemptDelay = retryPolicy.getNextAttemptDelay(attempt)
           messageView.incrementAndGetDeliveryAttempt()
           future = service.consume(messageView, nextAttemptDelay)
             └─ reschedules listener call after delay (local retry, no broker involved)
           recursively calls eraseFifoMessage()

         else if SUCCESS:
           ackMessage(mv) → AckMessageRequest

         else if FAILURE && attempt >= maxAttempts:
           log "run out of attempt times"
           forwardToDeadLetterQueue(mv)
             → ForwardMessageToDeadLetterQueueRequest → proxy
               → ProducerProcessor.forwardMessageToDeadLetterQueue()
                    delayLevel=-1, maxReconsumeTimes=0
                    → CONSUMER_SEND_MSG_BACK → broker → %DLQ%group

       → evictCache(messageView)
       → consumeIteratively() for next message  [strict sequential ordering]
```

**Key**: FIFO retries stay **within the same client process** via local delay.
No `changeInvisibleDuration` is called during retry — the message remains invisible
until the client either ACKs it or DLQs it (or the invisibleTime expires at broker).

### Retry policy (v5 SDK)

```java
ExponentialBackoffRetryPolicy:
  delay(attempt) = min(initialBackoff * multiplier^(attempt-1), maxBackoff)

// Server-pushed defaults (via settings negotiation):
//   initialBackoff = 1s, maxBackoff = varies, multiplier = 2.0, maxAttempts = 16
```

`maxAttempts` is negotiated with the broker cluster at session start via `Settings.getRetryPolicy()`.

### On receive: proxy filters standard messages before delivering to client

```
ReceiveMessageActivity.receiveMessage()
  │
  ├─ maxAttempts = settings.getBackoffPolicy().getMaxAttempts()
  │    ← client SDK setting, negotiated at session start
  │
  └─ ConsumerProcessor.popMessage(new PopMessageResultFilterImpl(maxAttempts))
       └─ for each message returned by broker POP:
            PopMessageResultFilterImpl.filterMessage()
              ├─ tag not matched?               → NO_MATCH  → ackMessage()
              ├─ reconsumeTimes >= maxAttempts? → TO_DLQ    ← DLQ trigger
              └─ else                           → MATCH     → deliver to client

       [if TO_DLQ]:
         ConsumerProcessor → messagingProcessor.forwardMessageToDeadLetterQueue()
           └─ ProducerProcessor.forwardMessageToDeadLetterQueue()
                consumerSendMsgBackRequestHeader.setDelayLevel(-1)        // forces DLQ at broker
                consumerSendMsgBackRequestHeader.setMaxReconsumeTimes(0)  // irrelevant when delayLevel<0
                → CONSUMER_SEND_MSG_BACK to broker

                .whenComplete: ackMessage()    // ACK original POP handle → no revive

  Broker: AbstractSendMessageProcessor.consumerSendMsgBack()
    delayLevel < 0 → isDLQ = true
    newTopic = "%DLQ%group"
    masterBroker.getMessageStore().putMessage(msgInner)
```

**DLQ trigger**: `reconsumeTimes >= maxAttempts` (client-configured `BackoffPolicy.maxAttempts`)

The client **never sees the message** when it exceeds `maxAttempts`. The proxy intercepts it silently.

### On explicit client-initiated DLQ (gRPC API)

The v5 gRPC API exposes `ForwardMessageToDeadLetterQueue` RPC for clients to manually send a message to DLQ:

```
Client → ForwardMessageToDeadLetterQueueRequest(topic, group, receiptHandle, messageId)
  └─ ForwardMessageToDLQActivity.forwardMessageToDeadLetterQueue()
       └─ ProducerProcessor.forwardMessageToDeadLetterQueue()
            delayLevel = -1, maxReconsumeTimes = 0
            → CONSUMER_SEND_MSG_BACK(delayLevel=-1) → broker → %DLQ%group
```

---

## Retry delay schedule

### Classic push retry (delay topic levels)

| Attempt | `reconsumeTimes` | Delay level | Delay |
|---|---|---|---|
| 1st fail | 0 | 3 | 10s |
| 2nd fail | 1 | 4 | 30s |
| 3rd fail | 2 | 5 | 1min |
| 4th fail | 3 | 6 | 2min |
| 5th fail | 4 | 7 | 3min |
| ... | ... | ... | ... |
| 16th fail | 15 | 18 | 2h |
| 17th fail | 16 | ≥ maxReconsumeTimes → **DLQ** | — |

### POP push retry (`changeInvisibleTime` levels, seconds)

```
popDelayLevel = [10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200]
```

`delayLevel = reconsumeTimes`, capped at last index (7200s = 2h).

---

## POP message full lifecycle (normal path, no DLQ)

```
T+0s      Consumer POPs message
          ├─ Broker: CK written to in-memory buffer (PopBufferMergeService)
          │     CK.reviveTime = T + invisibleTime (default 60s)
          └─ commitOffsets queue: [CK → nextBeginOffset=N+1]

T+0~10ms  Consumer processes message successfully
          └─ Consumer sends ACK
               ├─ Fast path: PopBufferMergeService.addAk() sets bit in CK bitmap
               └─ Slow path (buffer miss): ACK written to revive topic

T+5ms     PopBufferMergeService.scan()
          └─ isCkDone? (all bits set) → commitOffset(group, topic, queueId, N+1)
               ← original topic consumer offset advances

T+60s     [if no ACK] CK.reviveTime expires
          └─ PopReviveService.mergeAndRevive()
               └─ un-acked bits → reviveRetry() → %RETRY%group (re-delivery)
```

---

## POP CK buffer eviction

```
PopBufferMergeService.scan() runs every 5ms:

  For each CK in buffer:
    ├─ isCkDone (all ACKed, in-buffer)              → remove from buffer
    ├─ reviveTime - now < 3s (popCkStayBufferTimeOut) → removeCk = true
    └─ now - popTime > 10s (popCkStayBufferTime)    → removeCk = true

  [if removeCk]:
    ├─ putCkToStore()       → write CK to revive topic (disk)
    ├─ putAckToStore()      → write acked bits to revive topic (disk)
    └─ if isCkDoneForFinish && ckStored → remove from buffer

  scanCommitOffset():
    └─ for each (topic@group@qId) commitOffsets queue (ordered by arrival):
         peek head → ready? → commitOffset(nextBeginOffset) → queue.poll()
         not ready → break  (must commit in-order)
```

---

## ReviveService re-delivery (slow path)

```
PopReviveService.run() every reviveInterval=1s (master only):

  consumeReviveMessage():
    ├─ read revive topic from lastConsumedOffset
    ├─ CK_TAG     → ckMap.put(mergeKey, ck)
    ├─ ACK_TAG    → ckMap[mergeKey].setBit(msgIndex)
    └─ BATCH_ACK_TAG → ckMap[mergeKey].setBit for each offset

  mergeAndRevive():
    └─ for each CK sorted by reviveOffset:
         guard: endTime - ck.reviveTime > 2s         // not too early
         guard: normalTopic exists
         guard: subscriptionGroup exists
         guard: inflightReviveRequestMap.size() <= 3  // back-pressure

         reviveMsgFromCk():
           └─ for each un-acked bit:
                getBizMessage() → reviveRetry()
                  → put to %RETRY%group  ← consumer re-polls this next POP

    commitOffset(REVIVE_GROUP, reviveTopic, queueId, newOffset)
    ← revive topic offset only; original topic offset NOT touched here
```

**Note**: `PopReviveService` only commits the revive topic's own offset. The original topic's consumer offset is managed exclusively by `PopBufferMergeService.commitOffset()`.

---

## `rePutCK` — when reviveRetry fails (infra error)

```
rePutCK(oldCK, pair):
  rePutTimes = oldCK.parseRePutTimes()

  if rePutTimes >= 17 AND skipWhenCKRePutReachMaxTimes:
    drop silently  ← message lost, no DLQ

  else:
    newCk.invisibleTime = oldCK.invisibleTime
        + ckRewriteIntervalsInSeconds[rePutTimes] * 1000
    // intervals (s): [10,20,30,60,120,180,240,300,360,420,480,540,600,1200,1800,3600,7200]

    brokerController.getMessageStore().putMessage(newCk)
    ← reschedule CK in revive topic, try again later
```
