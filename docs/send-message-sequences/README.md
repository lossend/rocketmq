# RocketMQ Java Producer 发送时序图

本目录包含 `rocketmq-clients` Java Producer 到 `rocketmq` proxy/broker/store 的发送时序图，覆盖普通消息、事务消息、顺序消息、延迟消息，以及四类消息各自的客户端发送重试。

整体预览：`preview.png`。每张时序图都用 PlantUML `box` 标出 participant 所属边界：Client / Proxy / Broker / Store / CommitLog / Disk。

## 图片

| 场景 | SVG | PNG | 源文件 |
|---|---|---|---|
| 普通消息发送 | `normal-send.svg` | `normal-send.png` | `normal-send.puml` |
| 事务消息发送 | `transaction-send.svg` | `transaction-send.png` | `transaction-send.puml` |
| 顺序消息发送 | `fifo-send.svg` | `fifo-send.png` | `fifo-send.puml` |
| 延迟消息发送 | `delay-send.svg` | `delay-send.png` | `delay-send.puml` |
| 发送重试总览 | `send-retry.svg` | `send-retry.png` | `send-retry.puml` |
| 普通消息发送重试 | `normal-send-retry.svg` | `normal-send-retry.png` | `normal-send-retry.puml` |
| 事务消息发送重试 | `transaction-send-retry.svg` | `transaction-send-retry.png` | `transaction-send-retry.puml` |
| 顺序消息发送重试 | `fifo-send-retry.svg` | `fifo-send-retry.png` | `fifo-send-retry.puml` |
| 延迟消息发送重试 | `delay-send-retry.svg` | `delay-send-retry.png` | `delay-send-retry.puml` |

## 关键差异

- 普通消息：`PublishingMessageImpl` 判定为 `NORMAL`，proxy 只做基础校验和队列选择，broker 直接写 `DefaultMessageStore -> CommitLog`。
- 事务消息：第一次发送的是半消息。proxy 设置 `TRANSACTION_PREPARED_TYPE` 和 `PROPERTY_TRANSACTION_PREPARED=true`；broker 写入 half topic。用户后续 `commit/rollback` 会通过 `EndTransaction` 触发 broker 写最终消息或删除半消息。
- 顺序消息：客户端按 `messageGroup` hash 出固定 queue；proxy 也把 `messageGroup` 转成 `PROPERTY_SHARDING_KEY` 并按 sharding key 选写队列。同一 group 的重试仍固定在该队列。
- 延迟消息：客户端发送 `deliveryTimestamp`；proxy 转成 `PROPERTY_TIMER_DELIVER_MS`，必要时也设置 delay level；broker put hook 把消息改写到 timer wheel topic 或 `SCHEDULE_TOPIC_XXXX`，到期后再异步恢复真实 topic。
- 发送重试：重试发生在 `ProducerImpl.send0(...)`。非限流异常立即重试，`TooManyRequestsException` 走 retry policy 延迟重试；超过 `maxAttempts` 后向调用方返回异常。普通/延迟/事务半消息会在候选队列中轮转，顺序消息因 `messageGroup` 固定到单队列，重试也不换队列。

## 源码依据

- `rocketmq-clients/java/client/src/main/java/org/apache/rocketmq/client/java/impl/producer/ProducerImpl.java`
- `rocketmq-clients/java/client/src/main/java/org/apache/rocketmq/client/java/message/PublishingMessageImpl.java`
- `rocketmq-clients/java/client/src/main/java/org/apache/rocketmq/client/java/impl/producer/PublishingLoadBalancer.java`
- `rocketmq-clients/java/client/src/main/java/org/apache/rocketmq/client/java/impl/producer/TransactionImpl.java`
- `rocketmq/proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/producer/SendMessageActivity.java`
- `rocketmq/proxy/src/main/java/org/apache/rocketmq/proxy/processor/ProducerProcessor.java`
- `rocketmq/proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/transaction/EndTransactionActivity.java`
- `rocketmq/proxy/src/main/java/org/apache/rocketmq/proxy/processor/TransactionProcessor.java`
- `rocketmq/broker/src/main/java/org/apache/rocketmq/broker/processor/SendMessageProcessor.java`
- `rocketmq/broker/src/main/java/org/apache/rocketmq/broker/processor/EndTransactionProcessor.java`
- `rocketmq/broker/src/main/java/org/apache/rocketmq/broker/util/HookUtils.java`
- `rocketmq/store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java`
- `rocketmq/store/src/main/java/org/apache/rocketmq/store/CommitLog.java`
