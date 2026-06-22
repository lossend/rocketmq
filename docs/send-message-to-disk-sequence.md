# RocketMQ Send 到服务端磁盘时序图

这份图从 `rocketmq-clients` 的 Java `ProducerImpl.send(Message)` 开始，串到 `rocketmq` 仓库里的 proxy、broker、store 和最终 commitlog 刷盘路径。

> 说明：这张图按普通单条消息主路径绘制。事务、批量、定时、DLedger、HA 多副本等待和异常重试只保留关键分支或注释，不展开所有细节。

可直接查看的图片产物：

- `docs/send-message-to-disk-sequence.svg`
- `docs/send-message-to-disk-sequence.png`

可编辑源文件：

- `docs/send-message-to-disk-sequence.mmd`
- `docs/send-message-to-disk-sequence.puml`

```mermaid
%%{init: {"theme": "base", "themeVariables": {"background": "#ffffff", "primaryColor": "#ffffff", "primaryBorderColor": "#d1d5db", "primaryTextColor": "#111827", "lineColor": "#2563eb", "actorBorder": "#d1d5db", "actorBkg": "#ffffff", "actorTextColor": "#111827", "activationBkgColor": "#eff6ff", "activationBorderColor": "#2563eb", "noteBkgColor": "#f9fafb", "noteBorderColor": "#d1d5db", "noteTextColor": "#111827", "fontFamily": "Helvetica Neue, Arial, PingFang SC, Microsoft YaHei, sans-serif"}}}%%
sequenceDiagram
    autonumber
    actor App as 应用线程
    participant Producer as ProducerImpl<br/>rocketmq-clients
    participant CM as ClientManagerImpl<br/>RpcClientImpl
    participant ProxyGrpc as GrpcMessagingApplication<br/>rocketmq/proxy
    participant ProxySend as SendMessageActivity<br/>ProducerProcessor
    participant MsgSvc as MessageService<br/>Cluster/Local
    participant Broker as SendMessageProcessor<br/>rocketmq/broker
    participant Store as DefaultMessageStore
    participant CL as CommitLog
    participant MF as MappedFileQueue<br/>MappedFile
    participant Flush as FlushManager<br/>Flush services
    participant Disk as 服务端磁盘<br/>commitlog files
    participant Reput as ReputMessageService<br/>CQ/Index dispatch

    App->>Producer: send(Message) at ProducerImpl.java:214
    activate Producer
    Producer->>Producer: send(singletonList(message), false)
    Producer->>Producer: build PublishingMessageImpl<br/>校验 topic/type/FIFO group
    Producer->>Producer: getPublishingLoadBalancer(topic)<br/>选择 MessageQueue 和 broker endpoints
    Producer->>CM: sendMessage(endpoints, SendMessageRequest, timeout)
    activate CM
    CM->>CM: client.sign(); getRpcClient(endpoints)
    CM->>ProxyGrpc: gRPC MessagingService.SendMessage(request)
    deactivate CM

    activate ProxyGrpc
    ProxyGrpc->>ProxySend: producerThreadPoolExecutor 执行 sendMessage(ctx, request)
    activate ProxySend
    ProxySend->>ProxySend: validate topic/body/properties<br/>v2 Message -> common Message
    ProxySend->>ProxySend: select queue; set uniqId;<br/>build SendMessageRequestHeader
    ProxySend->>MsgSvc: sendMessage(ctx, AddressableMessageQueue, messages, header)
    activate MsgSvc

    alt Proxy cluster mode
        MsgSvc->>Broker: MQClientAPIExt.invoke<br/>RequestCode.SEND_MESSAGE_V2 + body
    else Proxy local mode
        MsgSvc->>Broker: brokerController.getSendMessageProcessor().processRequest(...)
    end
    deactivate MsgSvc

    activate Broker
    Broker->>Broker: parse header; hooks before;<br/>build MessageExtBrokerInner
    Broker->>Broker: set body/properties/tagsCode<br/>born/store host, reconsumeTimes

    alt brokerConfig.asyncSendEnable = true
        Broker->>Store: asyncPutMessage(msgInner)
        activate Store
        Store->>CL: asyncPutMessage(msgInner)
    else brokerConfig.asyncSendEnable = false
        Broker->>Store: putMessage(msgInner)
        activate Store
        Store->>CL: asyncPutMessage(msgInner)<br/>并 wait future
    end

    activate CL
    CL->>CL: set storeTimestamp/bodyCRC/version<br/>assign queue offset; encode
    CL->>MF: get or create last mapped file
    activate MF
    CL->>MF: appendMessage(msg, appendCallback)
    MF->>MF: write encoded message to mmap<br/>or transient writeBuffer/FileChannel
    MF-->>CL: AppendMessageResult(wroteOffset, wroteBytes, queueOffset)
    deactivate MF
    CL->>CL: increase queue offset; build PutMessageResult

    alt SYNC_FLUSH and waitStoreMsgOK
        CL->>Flush: GroupCommitService.putRequest(nextOffset)
        activate Flush
        Flush->>MF: mappedFileQueue.flush(0)
        activate MF
        MF->>Disk: mappedByteBuffer.force()<br/>or fileChannel.force(false)
        Disk-->>MF: data durable to commitlog file
        MF-->>Flush: flushedWhere >= nextOffset
        deactivate MF
        Flush-->>CL: future PUT_OK or FLUSH_DISK_TIMEOUT
        deactivate Flush
    else ASYNC_FLUSH
        CL->>Flush: wakeup flush or commit service
        activate Flush
        Flush-->>CL: completed PUT_OK<br/>producer 不等待磁盘 force
        Flush-->>MF: later: commit writeBuffer to FileChannel
        Flush-->>MF: later: mappedFileQueue.flush(...)
        MF-->>Disk: later: force data to commitlog file
        deactivate Flush
    end

    CL-->>Store: PutMessageResult
    deactivate CL
    Store-->>Broker: PutMessageResult
    deactivate Store

    Broker->>Broker: handlePutMessageResult<br/>set response code and offsets
    Broker-->>ProxySend: SendResult / Remoting response
    deactivate Broker
    ProxySend-->>ProxyGrpc: SendMessageResponse(entries)
    deactivate ProxySend
    ProxyGrpc-->>CM: gRPC SendMessageResponse
    deactivate ProxyGrpc
    CM-->>Producer: SendMessageResponse
    Producer->>Producer: SendReceiptImpl.processResponseInvocation
    Producer-->>App: SendReceipt(messageId, queueOffset, recallHandle)
    deactivate Producer

    par 消费索引异步构建
        Reput->>CL: getData(reputFromOffset)
        CL-->>Reput: commitlog bytes
        Reput->>Reput: checkMessageAndReturnSize -> DispatchRequest
        Reput->>Store: doDispatch(dispatchRequest)
        Store->>Disk: ConsumeQueue / Index files are updated asynchronously
    end
```

## 关键结论

- `ProducerImpl.send(Message)` 在 `rocketmq-clients` 里不是直接访问 broker remoting，而是把消息包装成 v2 `SendMessageRequest`，通过 gRPC `MessagingService.SendMessage` 发往 proxy。
- `rocketmq` 的 proxy 收到 gRPC 请求后，把 v2 message 转成 common `Message`，构造 `SendMessageRequestHeader`，再通过 `MessageService` 发往 broker。
- cluster proxy mode 通过 `MQClientAPIExt.sendMessageAsync` 发送 `RequestCode.SEND_MESSAGE_V2` 到目标 broker；local proxy mode 会直接调用 `brokerController.getSendMessageProcessor().processRequest(...)`。
- broker 的 `SendMessageProcessor` 把请求转成 `MessageExtBrokerInner` 后调用 `DefaultMessageStore.asyncPutMessage` 或 `putMessage`。
- 真正写消息主体的是 `CommitLog.asyncPutMessage`：编码后在 `putMessageLock` 内调用 `MappedFile.appendMessage`，写入 mmap 或 transient write buffer/FileChannel。
- 默认 commitlog 目录是 `${user.home}/store/commitlog`，单个 commitlog 文件默认 1 GiB，文件名是物理偏移量。
- `SYNC_FLUSH + waitStoreMsgOK` 会等待 `GroupCommitService` 把目标 offset flush 到磁盘；`ASYNC_FLUSH` 只唤醒后台刷盘/commit 线程，producer 成功返回时不保证已经 `force()` 到磁盘。
- `ConsumeQueue` 和 `Index` 不是 send 主线程直接写完的消息主体，它们由 `ReputMessageService` 后续扫描 commitlog 生成，用于消费定位和查询。

## 源码定位

- `/Users/lossend/opensource/rocketmq-clients/java/client/src/main/java/org/apache/rocketmq/client/java/impl/producer/ProducerImpl.java:214`
- `/Users/lossend/opensource/rocketmq-clients/java/client/src/main/java/org/apache/rocketmq/client/java/impl/ClientManagerImpl.java:229`
- `/Users/lossend/opensource/rocketmq-clients/java/client/src/main/java/org/apache/rocketmq/client/java/rpc/RpcClientImpl.java:142`
- `/Users/lossend/opensource/rocketmq/proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/GrpcMessagingApplication.java:239`
- `/Users/lossend/opensource/rocketmq/proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/producer/SendMessageActivity.java:66`
- `/Users/lossend/opensource/rocketmq/proxy/src/main/java/org/apache/rocketmq/proxy/processor/ProducerProcessor.java:69`
- `/Users/lossend/opensource/rocketmq/proxy/src/main/java/org/apache/rocketmq/proxy/service/message/ClusterMessageService.java:65`
- `/Users/lossend/opensource/rocketmq/proxy/src/main/java/org/apache/rocketmq/proxy/service/message/LocalMessageService.java:94`
- `/Users/lossend/opensource/rocketmq/client/src/main/java/org/apache/rocketmq/client/impl/mqclient/MQClientAPIExt.java:172`
- `/Users/lossend/opensource/rocketmq/broker/src/main/java/org/apache/rocketmq/broker/processor/SendMessageProcessor.java:89`
- `/Users/lossend/opensource/rocketmq/broker/src/main/java/org/apache/rocketmq/broker/processor/SendMessageProcessor.java:243`
- `/Users/lossend/opensource/rocketmq/store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java:647`
- `/Users/lossend/opensource/rocketmq/store/src/main/java/org/apache/rocketmq/store/CommitLog.java:969`
- `/Users/lossend/opensource/rocketmq/store/src/main/java/org/apache/rocketmq/store/logfile/DefaultMappedFile.java:351`
- `/Users/lossend/opensource/rocketmq/store/src/main/java/org/apache/rocketmq/store/logfile/DefaultMappedFile.java:526`
- `/Users/lossend/opensource/rocketmq/store/src/main/java/org/apache/rocketmq/store/CommitLog.java:2184`
- `/Users/lossend/opensource/rocketmq/store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java:2713`
