# RocketMQ 服务端插件与扩展点

本文档列出当前源码中的 RocketMQ 服务端扩展点，覆盖 Broker、NameServer、Controller、Proxy、Store、TieredStore、Auth、Remoting、Filter 和通用服务工具模块。

RocketMQ 服务端没有统一的插件注册中心。扩展点主要分为四类：

- 配置类名装载：通过配置项填写实现类全限定名，再由 `Class.forName` 创建。
- RocketMQ `ServiceProvider` 装载：读取 `META-INF/service/<接口全限定名>`，注意目录名是 `service`，不是 JDK 标准 `services`。
- JDK `ServiceLoader` 装载：读取 `META-INF/services/<接口全限定名>`。
- 代码注册或注入：通过 `register`、`append`、`set`、`addListener` 等方法接入。

## 扩展点总览

| 模块 | 扩展点 | 接入方式 | 主要入口 |
| --- | --- | --- | --- |
| Store | `MessageStore` 插件链 | `messageStorePlugIn` 配置类名 | `MessageStoreFactory` |
| Broker | `TransactionalMessageService` | `META-INF/service/...TransactionalMessageService` | `BrokerController.initialTransaction` |
| Broker | `AbstractTransactionalMessageCheckListener` | `META-INF/service/...AbstractTransactionalMessageCheckListener` | `BrokerController.initialTransaction` |
| Remoting/Broker | `RPCHook` | `META-INF/service/...RPCHook` 或代码注册 | `BrokerController.initialRpcHooks`、`RemotingService.registerRPCHook` |
| Store | `HAService` | `META-INF/service/...HAService` | `DefaultMessageStore.initializeHAService` |
| Store | `MappedFile` | JDK `ServiceLoader` | `AllocateMappedFileService` |
| Auth | 认证/授权 provider、metadata provider、strategy | `AuthConfig` 类名配置 | `AuthenticationFactory`、`AuthorizationFactory` |
| TieredStore | `MetadataStore` | `tieredMetadataServiceProvider` 类名配置 | `TieredMessageStore` |
| TieredStore | `FileSegment` / `FileSegmentProvider` | `tieredBackendServiceProvider` 类名配置 | `FileSegmentFactory` |
| Broker | `SendMessageHook` / `ConsumeMessageHook` | 代码注册 | `BrokerController.registerSendMessageHook`、`registerConsumeMessageHook` |
| Broker | `BrokerAttachedPlugin` | 代码注入插件列表 | `BrokerController.getBrokerAttachedPlugins` |
| Broker | `PullMessageResultHandler` | `PullMessageProcessor.setPullMessageResultHandler` | `PullMessageProcessor` |
| Store | `PutMessageHook`、`SendMessageBackHook`、`MessageArrivingListener` | 代码注册或构造注入 | `DefaultMessageStore`、`BrokerController.registerMessageStoreHook` |
| Store HA | `HAReadHook`、`HAWriteHook` | 代码注册 | HA reader/writer |
| Store | mapped file 预处理和 timer escape bridge | 代码注册 | `AllocateMappedFileService`、`TimerMessageStore` |
| Remoting | `NettyRequestProcessor` | `RemotingServer.registerProcessor` | Broker/NameServer/Controller/Proxy processor 注册 |
| Remoting | `ChannelEventListener` | Netty server/client 配置注入 | housekeeping service |
| Remoting | `RequestPipeline` | pipeline 链式组合 | Broker auth pipeline |
| Remoting | `RpcClientHook` | `RpcClientImpl.registerHook` | RPC client 出站钩子 |
| Remoting | TLS 私钥解密策略 | `TlsHelper.registerDecryptionStrategy` | `TlsHelper` |
| Filter | `FilterSpi` | `FilterFactory.register` | `ConsumerFilterManager` |
| Filter | `MessageFilter` | 拉取、pop、store 查询路径注入 | `PullMessageProcessor`、store 查询 |
| Controller | `BrokerLifecycleListener` | `registerBrokerLifecycleListener` | `Controller`、`BrokerHeartbeatManager` |
| Controller | `ElectPolicy` | 构造或 setter 注入 | `DLedgerController.setElectPolicy` |
| NameServer | 内嵌 Controller | `enableControllerInNamesrv=true` | `NamesrvStartup.controllerManagerMain` |
| Proxy | `ServiceManagerFactory` / `ServiceManager` | Local/Cluster 模式构造 | `DefaultMessagingProcessor` |
| Proxy | gRPC 服务和拦截器 | `GrpcServerBuilder.addService`、`appendInterceptor` | `ProxyStartup` |
| Proxy | 多协议处理器 | `ProtocolNegotiationHandler.addProtocolHandler` | `MultiProtocolRemotingServer` |
| Proxy | 请求 pipeline | Remoting/gRPC pipeline 链式组合 | `RemotingProtocolServer`、`GrpcMessagingApplication` |
| Proxy | TLS reload listener | `TlsCertificateManager.registerReloadListener` | `GrpcServer`、`RemotingProtocolServer` |
| Proxy | 队列选择、优先级和惩罚器 | `QueueSelector`、`TopicRouteService.setPriorityProvider`、`addPenalizer` | Proxy send/pop 路由 |
| Proxy | pop 结果过滤 | `PopMessageResultFilter` | `ConsumerProcessor.popMessage` |
| Proxy | Topic 消息类型校验 | `TopicMessageTypeValidator` | send/recall activity |
| Common | `StateEventListener` | 构造注入 | receipt handle 状态事件 |
| Common | `FileWatchService.Listener` | 构造注入 | TLS/证书/配置文件变更监听 |
| Common | `Handler<T,R>` | `HandlerChain` 组合 | Auth 认证授权链 |
| Common | `RetryPolicy` | subscription group retry policy | Broker/Proxy 重试和续期计算 |

## 1. 装载与注册约定

### 1.1 RocketMQ ServiceProvider

定义：RocketMQ 自有的轻量 SPI 装载器。资源文件路径为 `META-INF/service/<接口全限定名>`，文件内容为实现类全限定名，每行一个。`load(Class)` 返回所有实现实例，`loadClass(Class)` 返回第一个实现类实例。

涉及扩展：

- `TransactionalMessageService`
- `AbstractTransactionalMessageCheckListener`
- `RPCHook`
- `HAService`

```java
/**
 * Loads RocketMQ server-side service implementations from META-INF/service resources.
 *
 * <p>The resource path is {@code META-INF/service/<interface-fqcn>}. Each non-empty line
 * names one implementation class that must be visible to the context class loader.</p>
 */
public final class ServiceProvider {

    /**
     * Loads all implementations declared for the target service interface.
     *
     * @param clazz service interface or abstract base class
     * @param <T> implementation type
     * @return instantiated implementations, or an empty list when no resource exists
     */
    public static <T> List<T> load(Class<?> clazz);

    /**
     * Loads the first implementation declared for the target service interface.
     *
     * @param clazz service interface or abstract base class
     * @param <T> implementation type
     * @return instantiated implementation, or null when no implementation exists
     */
    public static <T> T loadClass(Class<?> clazz);
}
```

### 1.2 配置类名装载

定义：配置项保存实现类全限定名，运行时通过 `Class.forName` 加载，再按约定构造。

涉及扩展：

- `messageStorePlugIn`
- `authenticationProvider`
- `authenticationMetadataProvider`
- `authenticationStrategy`
- `authorizationProvider`
- `authorizationMetadataProvider`
- `authorizationStrategy`
- `tieredMetadataServiceProvider`
- `tieredBackendServiceProvider`

### 1.3 代码注册或注入

定义：运行时通过 setter、register、append、addListener 等方法把实现对象传入服务端组件。此类扩展通常是内部扩展面或嵌入式使用场景，不一定有配置项。

## 2. Store 扩展点

### 2.1 MessageStore 插件链

定义：`MessageStore` 的装饰器式插件链。每个插件继承 `AbstractPluginMessageStore`，构造函数必须是 `(MessageStorePluginContext, MessageStore)`。未覆写的方法默认透传给 `next`。

接入方式：在 broker 配置中设置 `messageStorePlugIn`，多个插件用英文逗号分隔。`MessageStoreFactory` 按配置顺序构建包装链。

```java
/**
 * Base class for MessageStore plugins.
 *
 * <p>A plugin wraps another {@link MessageStore}. Override only the methods that need
 * custom behavior and delegate to {@code next} for the rest.</p>
 */
public abstract class AbstractPluginMessageStore implements MessageStore {

    /**
     * The next store in the plugin chain.
     */
    protected MessageStore next;

    /**
     * Broker and store context available to the plugin.
     */
    protected MessageStorePluginContext context;

    /**
     * Creates a MessageStore plugin.
     *
     * @param context broker/store context and configuration access
     * @param next next MessageStore in the chain
     */
    public AbstractPluginMessageStore(MessageStorePluginContext context, MessageStore next);
}
```

```java
/**
 * Context object passed to MessageStore plugins.
 */
public class MessageStorePluginContext {

    /**
     * Returns the store-level configuration.
     *
     * @return message store configuration
     */
    public MessageStoreConfig getMessageStoreConfig();

    /**
     * Returns broker store statistics manager.
     *
     * @return broker statistics manager
     */
    public BrokerStatsManager getBrokerStatsManager();

    /**
     * Returns the listener used to notify long-polling and pop paths of new messages.
     *
     * @return message arriving listener
     */
    public MessageArrivingListener getMessageArrivingListener();

    /**
     * Returns broker-level configuration.
     *
     * @return broker configuration
     */
    public BrokerConfig getBrokerConfig();

    /**
     * Registers a plugin-specific configuration object with broker configuration management.
     *
     * @param config configuration bean to populate and register
     */
    public void registerConfiguration(Object config);
}
```

### 2.2 PutMessageHook

定义：消息写入 `MessageStore` 前的同步钩子，可用于校验、改写或拒绝消息。

```java
/**
 * Hook invoked before a message is appended to the message store.
 */
public interface PutMessageHook {

    /**
     * Returns the unique hook name used for logging and diagnostics.
     *
     * @return hook name
     */
    String hookName();

    /**
     * Executes custom logic before storing the message.
     *
     * @param msg message to be stored
     * @return a non-null result to stop normal put processing, or null to continue
     */
    PutMessageResult executeBeforePutMessage(MessageExt msg);
}
```

### 2.3 SendMessageBackHook

定义：HA 握手或从节点回传消息时的回调。

```java
/**
 * Hook invoked when a slave sends messages back to the master during HA processing.
 */
public interface SendMessageBackHook {

    /**
     * Sends a list of messages back to the target broker address.
     *
     * @param msgList messages to send back
     * @param brokerName target broker name
     * @param brokerAddr target broker address
     * @return true when the messages were sent successfully
     */
    boolean executeSendMessageBack(List<MessageExt> msgList, String brokerName, String brokerAddr);
}
```

### 2.4 MessageArrivingListener

定义：新消息到达 consume queue 后通知等待中的 pull/pop 请求。

```java
/**
 * Listener notified when a new message arrives in a consume queue.
 */
public interface MessageArrivingListener {

    /**
     * Handles a message-arrival event for long polling and notification paths.
     *
     * @param topic topic name
     * @param queueId queue id
     * @param logicOffset logical consume queue offset
     * @param tagsCode tag hash code stored in consume queue
     * @param msgStoreTime message store timestamp
     * @param filterBitMap optional bloom filter bitmap
     * @param properties message properties
     */
    void arriving(String topic, int queueId, long logicOffset, long tagsCode,
        long msgStoreTime, byte[] filterBitMap, Map<String, String> properties);
}
```

### 2.5 Store MessageFilter

定义：store 读取路径的消息匹配接口，先按 consume queue 扩展信息过滤，再按 commit log 内容过滤。

```java
/**
 * Store-level message filter used by pull, pop and query paths.
 */
public interface MessageFilter {

    /**
     * Matches a message using consume queue metadata.
     *
     * @param tagsCode tag hash stored in consume queue
     * @param cqExtUnit consume queue extension unit, may contain filter bitmap
     * @return true when the message may match and should continue to commit-log filtering
     */
    boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit);

    /**
     * Matches a message using commit log data or decoded properties.
     *
     * @param msgBuffer message buffer in commit log, may be null in non-store callers
     * @param properties decoded message properties, may be null when the caller expects the filter to decode
     * @return true when the message matches
     */
    boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties);
}
```

### 2.6 HAService

定义：存储层主从复制服务抽象。默认实现由 broker/store 模式决定，也可通过 RocketMQ `ServiceProvider` 替换。

```java
/**
 * High-availability service abstraction for commit-log replication.
 */
public interface HAService {

    /**
     * Initializes the HA service with the owning message store.
     *
     * @param defaultMessageStore owning message store
     * @throws IOException when initialization fails
     */
    void init(DefaultMessageStore defaultMessageStore) throws IOException;

    /**
     * Starts background HA services and network components.
     *
     * @throws Exception when startup fails
     */
    void start() throws Exception;

    /**
     * Shuts down HA resources.
     */
    void shutdown();

    /**
     * Switches the replica to master mode.
     *
     * @param masterEpoch new master epoch
     * @return true when the role change succeeds
     * @throws RocksDBException when metadata persistence fails
     */
    default boolean changeToMaster(int masterEpoch) throws RocksDBException;

    /**
     * Refreshes master state when the previous role was already master.
     *
     * @param masterEpoch new master epoch
     * @return true when the role refresh succeeds
     */
    default boolean changeToMasterWhenLastRoleIsMaster(int masterEpoch);

    /**
     * Switches the replica to slave mode.
     *
     * @param newMasterAddr new master address
     * @param newMasterEpoch new master epoch
     * @param slaveId slave broker id
     * @return true when the role change succeeds
     */
    default boolean changeToSlave(String newMasterAddr, int newMasterEpoch, Long slaveId);

    /**
     * Refreshes slave state when the master address remains unchanged.
     *
     * @param newMasterAddr current master address
     * @param newMasterEpoch current master epoch
     * @return true when the role refresh succeeds
     */
    default boolean changeToSlaveWhenMasterNotChange(String newMasterAddr, int newMasterEpoch);

    /**
     * Updates the logical master address.
     *
     * @param newAddr new master address
     */
    void updateMasterAddress(String newAddr);

    /**
     * Updates the HA replication master address.
     *
     * @param newAddr new HA master address
     */
    void updateHaMasterAddress(String newAddr);

    /**
     * Returns the number of replicas considered in sync for the given master offset.
     *
     * @param masterPutWhere current master max physical offset
     * @return in-sync replica count, including master when applicable
     */
    int inSyncReplicasNums(long masterPutWhere);

    /**
     * Returns the active HA connection count.
     *
     * @return connection count holder
     */
    AtomicInteger getConnectionCount();

    /**
     * Submits a group commit request that should wait for HA replication.
     *
     * @param request group commit request
     */
    void putRequest(CommitLog.GroupCommitRequest request);

    /**
     * Submits a pre-online group connection state request.
     *
     * @param request connection state notification request
     */
    void putGroupConnectionStateRequest(HAConnectionStateNotificationRequest request);

    /**
     * Returns current HA connections.
     *
     * @return connection list
     */
    List<HAConnection> getConnectionList();

    /**
     * Returns the local HA client.
     *
     * @return HA client
     */
    HAClient getHAClient();

    /**
     * Returns the max offset pushed to slaves.
     *
     * @return max pushed offset holder
     */
    AtomicLong getPush2SlaveMaxOffset();

    /**
     * Builds runtime information for HA diagnostics.
     *
     * @param masterPutWhere current master max physical offset
     * @return HA runtime information
     */
    HARuntimeInfo getRuntimeInfo(long masterPutWhere);

    /**
     * Returns the wait/notify object used by HA coordination.
     *
     * @return wait notify object
     */
    WaitNotifyObject getWaitNotifyObject();

    /**
     * Tests whether slaves are close enough to the master offset.
     *
     * @param masterPutWhere current master max physical offset
     * @return true when slave replication lag is acceptable
     */
    boolean isSlaveOK(long masterPutWhere);
}
```

### 2.7 HAReadHook 与 HAWriteHook

定义：HA 网络读写后回调，主要用于内部计量或测试扩展。

```java
/**
 * Hook invoked after HA reader reads bytes from the channel.
 */
public interface HAReadHook {

    /**
     * Handles a completed read operation.
     *
     * @param readSize number of bytes read
     */
    void afterRead(int readSize);
}
```

```java
/**
 * Hook invoked after HA writer writes bytes to the channel.
 */
public interface HAWriteHook {

    /**
     * Handles a completed write operation.
     *
     * @param writeSize number of bytes written
     */
    void afterWrite(int writeSize);
}
```

### 2.8 MappedFile

定义：commit log、consume queue 等底层文件的抽象。`AllocateMappedFileService` 在 transient store pool 场景下尝试用 JDK `ServiceLoader` 装载自定义实现。

```java
/**
 * Abstraction for mapped storage files used by commit log and consume queues.
 */
public interface MappedFile {

    /** @return file name. */
    String getFileName();

    /** @param fileName new file name; @return true when rename succeeds. */
    boolean renameTo(String fileName);

    /** @return configured file size in bytes. */
    int getFileSize();

    /** @return underlying file channel. */
    FileChannel getFileChannel();

    /** @return true when the file has no writable space. */
    boolean isFull();

    /** @return true when the file is not shut down or destroyed. */
    boolean isAvailable();

    /** @return append result for a broker message. */
    AppendMessageResult appendMessage(MessageExtBrokerInner message, AppendMessageCallback messageCallback,
        PutMessageContext putMessageContext);

    /** @return append result for a batch message. */
    AppendMessageResult appendMessages(MessageExtBatch message, AppendMessageCallback messageCallback,
        PutMessageContext putMessageContext);

    /** @return append result for compaction byte-buffer data. */
    AppendMessageResult appendMessage(ByteBuffer byteBufferMsg, CompactionAppendMsgCallback cb);

    /** @return true when raw bytes were appended through mapped memory. */
    boolean appendMessage(byte[] data);

    /** @return true when raw bytes were appended through the file channel. */
    boolean appendMessageUsingFileChannel(byte[] data);

    /** @return true when raw bytes were appended from the buffer. */
    boolean appendMessage(ByteBuffer data);

    /** @return true when the selected byte range was appended. */
    boolean appendMessage(byte[] data, int offset, int length);

    /** @return physical offset represented by the file name. */
    long getFileFromOffset();

    /** @return flushed position after flushing at least the requested pages when possible. */
    int flush(int flushLeastPages);

    /** @return committed position after committing transient data. */
    int commit(int commitLeastPages);

    /** @return selected mapped buffer region for the requested range. */
    SelectMappedBufferResult selectMappedBuffer(int pos, int size);

    /** @return selected mapped buffer region from the requested position. */
    SelectMappedBufferResult selectMappedBuffer(int pos);

    /** @return underlying mapped byte buffer. */
    MappedByteBuffer getMappedByteBuffer();

    /** @return sliced byte buffer view. */
    ByteBuffer sliceByteBuffer();

    /** @return store timestamp of the last appended message. */
    long getStoreTimestamp();

    /** @return file last modified timestamp. */
    long getLastModifiedTimestamp();

    /** @return true when the requested data was copied into the target buffer. */
    boolean getData(int pos, int size, ByteBuffer byteBuffer);

    /** @return true when destroy and delete succeeded. */
    boolean destroy(long intervalForcibly);

    /** Marks the file unavailable and releases resources after the forced interval. */
    void shutdown(long intervalForcibly);

    /** Releases one reference to the file. */
    void release();

    /** @return true when a reference was acquired. */
    boolean hold();

    /** @return true when this is the first file in the queue. */
    boolean isFirstCreateInQueue();

    /** Sets whether this file is the first file in the queue. */
    void setFirstCreateInQueue(boolean firstCreateInQueue);

    /** @return flushed position. */
    int getFlushedPosition();

    /** Sets flushed position. */
    void setFlushedPosition(int flushedPosition);

    /** @return wrote position. */
    int getWrotePosition();

    /** Sets wrote position. */
    void setWrotePosition(int wrotePosition);

    /** @return current readable position. */
    int getReadPosition();

    /** Sets committed position. */
    void setCommittedPosition(int committedPosition);

    /** Locks mapped pages in memory when supported. */
    void mlock();

    /** Unlocks mapped pages from memory. */
    void munlock();

    /** Warms mapped pages according to flush type and page count. */
    void warmMappedFile(FlushDiskType type, int pages);

    /** @return true when swap mapping succeeds. */
    boolean swapMap();

    /** Cleans swapped page-table resources. */
    void cleanSwapedMap(boolean force);

    /** Cleans all file resources held by the implementation. */
    void cleanResources();

    /** @return most recent swap-map timestamp. */
    long getRecentSwapMapTime();

    /** @return mapped byte-buffer access count since last swap. */
    long getMappedByteBufferAccessCountSinceLastSwap();

    /** @return underlying file object. */
    File getFile();

    /** Renames the file to mark it for deletion. */
    void renameToDelete();

    /** Moves the file to the parent directory. */
    void moveToParent() throws IOException;

    /** @return last flush timestamp. */
    long getLastFlushTime();

    /** Initializes the mapped file. */
    void init(String fileName, int fileSize, RunningFlags runningFlags,
        TransientStorePool transientStorePool) throws IOException;

    /** @return iterator over mapped buffer slices starting at the position. */
    Iterator<SelectMappedBufferResult> iterator(int pos);

    /** @return true when the requested file range is resident in memory. */
    boolean isLoaded(long position, int size);
}
```

### 2.9 Mapped file 预处理

定义：分配 mapped file 请求入队前的预处理回调。

```java
/**
 * Callback invoked before AllocateMappedFileService enqueues mapped-file allocation.
 */
@FunctionalInterface
public interface PreprocessHandler {

    /**
     * Handles the two file paths that may be allocated and their file size.
     *
     * @param nextFilePath next mapped file path
     * @param nextNextFilePath following mapped file path
     * @param fileSize mapped file size
     */
    void preprocess(String nextFilePath, String nextNextFilePath, int fileSize);
}
```

### 2.10 Timer escape bridge hook

定义：timer message store 在需要把消息逃逸到普通写入路径时使用的函数式钩子。

```java
/**
 * Registers a bridge used by timer store to write escaped messages.
 *
 * @param escapeBridgeHook function that writes a broker-inner message and returns the put result
 */
public void registerEscapeBridgeHook(Function<MessageExtBrokerInner, PutMessageResult> escapeBridgeHook);
```

## 3. Broker 扩展点

### 3.1 TransactionalMessageService

定义：事务半消息的存储、提交、回滚、回查服务。

```java
/**
 * Service abstraction for transactional half-message lifecycle management.
 */
public interface TransactionalMessageService {

    /** Stores a transactional prepare, or half, message. */
    PutMessageResult prepareMessage(MessageExtBrokerInner messageInner);

    /** Stores a transactional prepare message asynchronously. */
    CompletableFuture<PutMessageResult> asyncPrepareMessage(MessageExtBrokerInner messageInner);

    /** Deletes a prepare message after commit or rollback. */
    boolean deletePrepareMessage(MessageExt messageExt);

    /** Commits a prepare message identified by the end-transaction request. */
    OperationResult commitMessage(EndTransactionRequestHeader requestHeader);

    /** Rolls back a prepare message identified by the end-transaction request. */
    OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader);

    /** Scans pending half messages and invokes the listener for check or discard actions. */
    void check(long transactionTimeout, int transactionCheckMax,
        AbstractTransactionalMessageCheckListener listener);

    /** Opens resources used by the transactional message service. */
    boolean open();

    /** Closes resources used by the transactional message service. */
    void close();

    /** Returns transaction metrics state. */
    TransactionMetrics getTransactionMetrics();

    /** Sets transaction metrics state. */
    void setTransactionMetrics(TransactionMetrics transactionMetrics);
}
```

### 3.2 AbstractTransactionalMessageCheckListener

定义：事务消息回查监听器。可替换 discard 行为，也可复用默认发送回查请求逻辑。

```java
/**
 * Listener used by transaction check service to resolve half messages.
 */
public abstract class AbstractTransactionalMessageCheckListener {

    /** Creates a listener without broker controller injection. */
    public AbstractTransactionalMessageCheckListener();

    /** Creates a listener bound to a broker controller. */
    public AbstractTransactionalMessageCheckListener(BrokerController brokerController);

    /** Sends a transaction-state check request to the producer. */
    public void sendCheckMessage(MessageExt msgExt) throws Exception;

    /** Submits a half message for asynchronous transaction check resolution. */
    public void resolveHalfMsg(MessageExt msgExt);

    /** Returns the injected broker controller. */
    public BrokerController getBrokerController();

    /** Shuts down the listener executor. */
    public void shutdown();

    /** Initializes the listener executor service when needed. */
    public synchronized void initExecutorService();

    /** Injects broker controller and initializes executor resources. */
    public void setBrokerController(BrokerController brokerController);

    /** Handles a half message that exceeded the max check count and should be discarded. */
    public abstract void resolveDiscardMsg(MessageExt msgExt);
}
```

### 3.3 SendMessageHook

定义：发送消息请求处理前后的 broker 钩子。

```java
/**
 * Hook around broker send-message processing.
 */
public interface SendMessageHook {

    /** Returns the hook name used for diagnostics. */
    String hookName();

    /** Runs before broker send-message processing. */
    void sendMessageBefore(SendMessageContext context);

    /** Runs after broker send-message processing. */
    void sendMessageAfter(SendMessageContext context);
}
```

### 3.4 ConsumeMessageHook

定义：消费相关处理前后的 broker 钩子，主要用于 trace 或审计。

```java
/**
 * Hook around broker consume-message related processing.
 */
public interface ConsumeMessageHook {

    /** Returns the hook name used for diagnostics. */
    String hookName();

    /** Runs before consume-message related processing. */
    void consumeMessageBefore(ConsumeMessageContext context);

    /** Runs after consume-message related processing. */
    void consumeMessageAfter(ConsumeMessageContext context);
}
```

### 3.5 BrokerAttachedPlugin

定义：Broker 附加插件生命周期接口。当前源码有调用点和插件列表，但没有 OSS 自动发现入口。

```java
/**
 * Lifecycle contract for plugins attached directly to a BrokerController.
 */
public interface BrokerAttachedPlugin {

    /** Returns the plugin name. */
    String pluginName();

    /** Loads plugin resources before broker start. */
    boolean load();

    /** Starts the plugin after broker initialization. */
    void start();

    /** Shuts down plugin resources. */
    void shutdown();

    /** Synchronizes plugin metadata from master to slave. */
    void syncMetadata();

    /** Synchronizes plugin metadata from slave back to a broker address. */
    void syncMetadataReverse(String brokerAddr) throws Exception;

    /** Adds plugin-specific runtime information to broker runtime info. */
    void buildRuntimeInfo(Map<String, String> runtimeInfo);

    /** Handles broker status or role changes. */
    void statusChanged(boolean shouldStart);
}
```

### 3.6 PullMessageResultHandler

定义：Pull 请求从 store 取到结果后的响应构造和挂起逻辑扩展。

```java
/**
 * Handles broker pull-message results after reading from the message store.
 */
public interface PullMessageResultHandler {

    /**
     * Builds or mutates the remoting response for a pull result.
     *
     * @param getMessageResult store read result
     * @param request original remoting request
     * @param requestHeader decoded pull request header
     * @param channel client channel
     * @param subscriptionData subscription data used for matching
     * @param subscriptionGroupConfig subscription group configuration
     * @param brokerAllowSuspend whether long polling is allowed
     * @param messageFilter message filter used by the read
     * @param response response being built
     * @param mappingContext static-topic mapping context
     * @param beginTimeMills request begin timestamp
     * @return response to send, or null when processing is suspended
     */
    RemotingCommand handle(GetMessageResult getMessageResult, RemotingCommand request,
        PullMessageRequestHeader requestHeader, Channel channel, SubscriptionData subscriptionData,
        SubscriptionGroupConfig subscriptionGroupConfig, boolean brokerAllowSuspend,
        MessageFilter messageFilter, RemotingCommand response,
        TopicQueueMappingContext mappingContext, long beginTimeMills);
}
```

### 3.7 ConsumerIdsChangeListener

定义：消费组客户端集合或订阅变化监听器。

```java
/**
 * Listener for consumer-id and consumer-group membership changes.
 */
public interface ConsumerIdsChangeListener {

    /** Handles a consumer group event. */
    void handle(ConsumerGroupEvent event, String group, Object... args);

    /** Releases listener resources. */
    void shutdown();
}
```

### 3.8 ProducerChangeListener

定义：生产者组客户端变化监听器。

```java
/**
 * Listener for producer group membership changes.
 */
public interface ProducerChangeListener {

    /** Handles a producer group event. */
    void handle(ProducerGroupEvent event, String group, ClientChannelInfo clientChannelInfo);
}
```

### 3.9 ShutdownHook

定义：Broker 关闭前回调。

```java
/**
 * Hook executed before BrokerController shutdown.
 */
public interface ShutdownHook {

    /** Executes custom cleanup before broker shutdown. */
    void beforeShutdown(BrokerController controller);
}
```

### 3.10 LiteCtlListener

定义：Lite/LMQ 订阅注册表监听器。

```java
/**
 * Listener for Lite subscription registry changes.
 */
public interface LiteCtlListener {

    /** Handles registration of an LMQ binding for a client and group. */
    void onRegister(String clientId, String group, String lmqName);

    /** Handles unregistration of an LMQ binding for a client and group. */
    void onUnregister(String clientId, String group, String lmqName);

    /** Handles removal of all LMQ bindings for a client and group. */
    void onRemoveAll(String clientId, String group);
}
```

### 3.11 ColdCtrStrategy

定义：冷数据读取限流策略。当前实现通过配置在内置 simple 和 PID adaptive 策略之间选择。

```java
/**
 * Strategy used to adapt cold-data read throttling.
 */
public interface ColdCtrStrategy {

    /** Calculates the control factor used to decide promotion or deceleration. */
    Double decisionFactor();

    /** Promotes cold-data read throughput for a consumer group. */
    void promote(String consumerGroup, Long currentThreshold);

    /** Decelerates cold-data read throughput for a consumer group. */
    void decelerate(String consumerGroup, Long currentThreshold);

    /** Collects the global cold-read accumulation value. */
    void collect(Long globalAcc);
}
```

## 4. Auth 扩展点

### 4.1 AuthenticationProvider

定义：认证上下文构造和认证执行接口。

```java
/**
 * Provider that creates authentication contexts and authenticates requests.
 *
 * @param <AuthenticationContext> concrete authentication context type
 */
public interface AuthenticationProvider<AuthenticationContext> {

    /** Initializes the provider with auth configuration and optional metadata service supplier. */
    void initialize(AuthConfig config, Supplier<?> metadataService);

    /** Authenticates the context asynchronously. */
    CompletableFuture<Void> authenticate(AuthenticationContext context);

    /** Creates an authentication context from a gRPC request. */
    AuthenticationContext newContext(Metadata metadata, GeneratedMessageV3 request);

    /** Creates an authentication context from a remoting request. */
    AuthenticationContext newContext(ChannelHandlerContext context, RemotingCommand command);
}
```

### 4.2 AuthenticationMetadataProvider

定义：认证用户元数据存储接口。

```java
/**
 * Provider for authentication user metadata.
 */
public interface AuthenticationMetadataProvider {

    /** Initializes metadata access. */
    void initialize(AuthConfig authConfig, Supplier<?> metadataService);

    /** Releases metadata resources. */
    void shutdown();

    /** Creates a user. */
    CompletableFuture<Void> createUser(User user);

    /** Deletes a user by username. */
    CompletableFuture<Void> deleteUser(String username);

    /** Updates a user. */
    CompletableFuture<Void> updateUser(User user);

    /** Gets a user by username. */
    CompletableFuture<User> getUser(String username);

    /** Lists users matching a filter. */
    CompletableFuture<List<User>> listUser(String filter);
}
```

### 4.3 AuthenticationStrategy

定义：认证策略评估接口，负责决定认证上下文是否通过。

```java
/**
 * Strategy that evaluates an authentication context.
 */
public interface AuthenticationStrategy {

    /** Evaluates authentication and throws on failure. */
    void evaluate(AuthenticationContext context);
}
```

### 4.4 AuthorizationProvider

定义：授权上下文构造和授权执行接口。

```java
/**
 * Provider that creates authorization contexts and authorizes requests.
 *
 * @param <AuthorizationContext> concrete authorization context type
 */
public interface AuthorizationProvider<AuthorizationContext> {

    /** Initializes the provider with auth configuration. */
    void initialize(AuthConfig config);

    /** Initializes the provider with auth configuration and optional metadata service supplier. */
    void initialize(AuthConfig config, Supplier<?> metadataService);

    /** Authorizes the context asynchronously. */
    CompletableFuture<Void> authorize(AuthorizationContext context);

    /** Creates authorization contexts from a gRPC request. */
    List<AuthorizationContext> newContexts(Metadata metadata, GeneratedMessageV3 message);

    /** Creates authorization contexts from a remoting request. */
    List<AuthorizationContext> newContexts(ChannelHandlerContext context, RemotingCommand command);
}
```

### 4.5 AuthorizationMetadataProvider

定义：授权 ACL 元数据存储接口。

```java
/**
 * Provider for authorization ACL metadata.
 */
public interface AuthorizationMetadataProvider {

    /** Initializes metadata access. */
    void initialize(AuthConfig authConfig, Supplier<?> metadataService);

    /** Releases metadata resources. */
    void shutdown();

    /** Creates an ACL. */
    CompletableFuture<Void> createAcl(Acl acl);

    /** Deletes ACLs for a subject. */
    CompletableFuture<Void> deleteAcl(Subject subject);

    /** Updates an ACL. */
    CompletableFuture<Void> updateAcl(Acl acl);

    /** Gets an ACL by subject. */
    CompletableFuture<Acl> getAcl(Subject subject);

    /** Lists ACLs matching subject and resource filters. */
    CompletableFuture<List<Acl>> listAcl(String subjectFilter, String resourceFilter);
}
```

### 4.6 AuthorizationStrategy

定义：授权策略评估接口，负责决定授权上下文是否通过。

```java
/**
 * Strategy that evaluates an authorization context.
 */
public interface AuthorizationStrategy {

    /** Evaluates authorization and throws on failure. */
    void evaluate(AuthorizationContext context);
}
```

## 5. TieredStore 扩展点

### 5.1 MetadataStore

定义：TieredStore 的 topic、queue、file segment 元数据存储接口。

```java
/**
 * Metadata storage service for TieredStore topics, queues and file segments.
 */
public interface MetadataStore {

    /** Gets topic metadata, or null when absent. */
    TopicMetadata getTopic(String topic);

    /** Adds topic metadata. */
    TopicMetadata addTopic(String topic, long reserveTime);

    /** Updates topic metadata. */
    void updateTopic(TopicMetadata topicMetadata);

    /** Iterates over all topic metadata records. */
    void iterateTopic(Consumer<TopicMetadata> callback);

    /** Deletes topic metadata. */
    void deleteTopic(String topic);

    /** Gets queue metadata, or null when absent. */
    QueueMetadata getQueue(MessageQueue mq);

    /** Adds queue metadata. */
    QueueMetadata addQueue(MessageQueue mq, long baseOffset);

    /** Updates queue metadata. */
    void updateQueue(QueueMetadata queueMetadata);

    /** Iterates queue metadata for a topic. */
    void iterateQueue(String topic, Consumer<QueueMetadata> callback);

    /** Deletes queue metadata. */
    void deleteQueue(MessageQueue mq);

    /** Gets file segment metadata. */
    FileSegmentMetadata getFileSegment(String basePath, FileSegmentType fileType, long baseOffset);

    /** Updates file segment metadata. */
    void updateFileSegment(FileSegmentMetadata fileSegmentMetadata);

    /** Iterates over all file segment metadata records. */
    void iterateFileSegment(Consumer<FileSegmentMetadata> callback);

    /** Iterates file segment metadata under one base path and type. */
    void iterateFileSegment(String basePath, FileSegmentType fileType,
        Consumer<FileSegmentMetadata> callback);

    /** Deletes all file segment metadata under one base path and type. */
    void deleteFileSegment(String basePath, FileSegmentType fileType);

    /** Deletes one file segment metadata record. */
    void deleteFileSegment(String basePath, FileSegmentType fileType, long baseOffset);

    /** Destroys metadata resources. */
    void destroy();
}
```

### 5.2 FileSegmentProvider

定义：TieredStore 后端文件系统读写接口。自定义后端通常继承 `FileSegment` 并实现这些方法。

```java
/**
 * Backend storage operations for a TieredStore file segment.
 */
public interface FileSegmentProvider {

    /** Returns the backend file path. */
    String getPath();

    /** Returns backend file length, 0 if absent, or -1 on size lookup failure. */
    long getSize();

    /** Returns whether the backend file exists. */
    boolean exists();

    /** Creates the backend file. */
    void createFile();

    /** Destroys the backend file. */
    void destroyFile();

    /** Reads bytes from backend storage. */
    CompletableFuture<ByteBuffer> read0(long position, int length);

    /** Commits bytes to backend storage. */
    CompletableFuture<Boolean> commit0(FileSegmentInputStream inputStream, long position,
        int length, boolean append);
}
```

### 5.3 TieredStore MessageStoreFilter

定义：TieredStore dispatcher/fetcher 的 topic 黑名单过滤接口，当前为内部扩展面。

```java
/**
 * Topic filter used by TieredStore message-store components.
 */
public interface MessageStoreFilter {

    /** Returns true when a topic should be filtered out. */
    boolean filterTopic(String topicName);

    /** Adds a topic to the filter blacklist. */
    void addTopicToBlackList(String topicName);
}
```

## 6. Filter 扩展点

### 6.1 FilterSpi

定义：消息过滤表达式编译 SPI。默认注册 SQL92 过滤器。

```java
/**
 * SPI for compiling message filter expressions.
 */
public interface FilterSpi {

    /** Compiles a textual filter expression. */
    Expression compile(String expr) throws MQFilterException;

    /** Returns the expression type handled by this SPI. */
    String ofType();
}
```

### 6.2 Common MessageFilter

定义：通用消息对象过滤接口。

```java
/**
 * Common message filter for filtering decoded message objects.
 */
public interface MessageFilter {

    /** Returns whether the message matches the filter context. */
    boolean match(MessageExt msg, FilterContext context);
}
```

### 6.3 FilterCheckHook

定义：过滤检查钩子接口。当前主源码中未找到注册或调用路径，应视为未接线接口。

```java
/**
 * Hook for checking whether a raw message buffer matches a filter.
 */
public interface FilterCheckHook {

    /** Returns the hook name used for diagnostics. */
    String hookName();

    /** Returns whether the raw buffer matches under the unit-mode flag. */
    boolean isFilterMatched(boolean isUnitMode, ByteBuffer byteBuffer);
}
```

## 7. Remoting 与请求链扩展点

### 7.1 RPCHook

定义：Remoting 请求前后钩子。Broker 可通过 `ServiceProvider` 自动加载，也可通过代码注册。

```java
/**
 * Hook invoked around remoting request processing.
 */
public interface RPCHook {

    /** Runs before a request is processed or sent. */
    void doBeforeRequest(String remoteAddr, RemotingCommand request);

    /** Runs after a response is produced or received. */
    void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response);
}
```

### 7.2 NettyRequestProcessor

定义：Remoting 请求处理器。Broker、NameServer、Controller、Proxy 都通过该接口注册请求码处理逻辑。

```java
/**
 * Processor for remoting commands received by a Netty remoting server.
 */
public interface NettyRequestProcessor {

    /** Processes one remoting request and returns a response command. */
    RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception;

    /** Returns true when the processor should reject new requests. */
    boolean rejectRequest();
}
```

### 7.3 ChannelEventListener

定义：Remoting channel 生命周期监听器。

```java
/**
 * Listener for remoting channel lifecycle events.
 */
public interface ChannelEventListener {

    /** Handles channel connect. */
    void onChannelConnect(String remoteAddr, Channel channel);

    /** Handles channel close. */
    void onChannelClose(String remoteAddr, Channel channel);

    /** Handles channel exception. */
    void onChannelException(String remoteAddr, Channel channel);

    /** Handles channel idle. */
    void onChannelIdle(String remoteAddr, Channel channel);

    /** Handles channel active. */
    void onChannelActive(String remoteAddr, Channel channel);
}
```

### 7.4 Broker RequestPipeline

定义：Broker remoting 请求进入业务处理前的 pipeline。当前用于 authn/authz。

```java
/**
 * Broker remoting request pipeline stage.
 */
public interface RequestPipeline {

    /** Executes this pipeline stage. */
    void execute(ChannelHandlerContext ctx, RemotingCommand request) throws Exception;

    /** Returns a composed pipeline that runs source before this stage. */
    default RequestPipeline pipe(RequestPipeline source);
}
```

### 7.5 RpcClientHook

定义：RPC client 侧请求和响应钩子。服务端组件作为客户端调用其他节点时可用。

```java
/**
 * Hook around RPC client calls.
 */
public abstract class RpcClientHook {

    /** Runs before an RPC request; returning non-null short-circuits the call. */
    public abstract RpcResponse beforeRequest(RpcRequest rpcRequest) throws RpcException;

    /** Runs after an RPC response; returning non-null replaces the response. */
    public abstract RpcResponse afterResponse(RpcResponse rpcResponse) throws RpcException;
}
```

### 7.6 TLS DecryptionStrategy

定义：TLS 私钥解密策略，可替换默认的文件输入流读取行为。

```java
/**
 * Strategy for decrypting encrypted TLS private key files.
 */
public interface DecryptionStrategy {

    /** Returns an input stream over the decrypted private key. */
    InputStream decryptPrivateKey(String privateKeyEncryptPath, boolean forClient) throws IOException;
}
```

## 8. Controller 与 NameServer 扩展点

### 8.1 BrokerLifecycleListener

定义：Controller 发现 broker inactive 时的监听器。

```java
/**
 * Listener for broker lifecycle events observed by the controller.
 */
public interface BrokerLifecycleListener {

    /** Handles broker inactive event. */
    void onBrokerInactive(String clusterName, String brokerName, Long brokerId);
}
```

### 8.2 ElectPolicy

定义：Controller 选主策略。

```java
/**
 * Policy used by controller to elect a broker master.
 */
public interface ElectPolicy {

    /** Elects a new master broker id from sync-state and replica sets. */
    Long elect(String clusterName, String brokerName, Set<Long> syncStateBrokers,
        Set<Long> allReplicaBrokers, Long oldMaster, Long brokerId);
}
```

### 8.3 NameServer 内嵌 Controller

定义：NameServer 可通过配置启动内嵌 Controller。该扩展点是部署模式，不是 Java 接口。

```properties
# 是否在 NameServer 进程内启动 ControllerManager
enableControllerInNamesrv=true
```

## 9. Proxy 扩展点

### 9.1 ServiceManagerFactory 与 ObjectCreator

定义：Proxy 服务层可按 local/cluster 模式构造，并可注入出站 remoting client 创建器。

```java
/**
 * Factory for Proxy service-manager implementations.
 */
public class ServiceManagerFactory {

    /** Creates local-mode services backed by an embedded BrokerController. */
    public static ServiceManager createForLocalMode(BrokerController brokerController);

    /** Creates local-mode services with an optional outbound RPC hook. */
    public static ServiceManager createForLocalMode(BrokerController brokerController, RPCHook rpcHook);

    /** Creates cluster-mode services with default remoting client behavior. */
    public static ServiceManager createForClusterMode();

    /** Creates cluster-mode services with an outbound RPC hook. */
    public static ServiceManager createForClusterMode(RPCHook rpcHook);

    /** Creates cluster-mode services with outbound RPC hook and remoting client factory. */
    public static ServiceManager createForClusterMode(RPCHook rpcHook,
        ObjectCreator<RemotingClient> remotingClientCreator);
}
```

```java
/**
 * Generic object factory used by extension-friendly constructors.
 *
 * @param <T> created object type
 */
public interface ObjectCreator<T> {

    /** Creates an object from caller-provided arguments. */
    T create(Object... args);
}
```

### 9.2 ServiceManager

定义：Proxy 服务集合抽象。它主要是内部服务组合接口，可用于替换 Proxy 后端服务组合。

```java
/**
 * Aggregates Proxy service components and controls their lifecycle.
 */
public interface ServiceManager extends StartAndShutdown {

    /** Returns message send, pull, pop and ack service. */
    MessageService getMessageService();

    /** Returns topic route service. */
    TopicRouteService getTopicRouteService();

    /** Returns producer manager. */
    ProducerManager getProducerManager();

    /** Returns consumer manager. */
    ConsumerManager getConsumerManager();

    /** Returns transaction service. */
    TransactionService getTransactionService();

    /** Returns relay service for broker-facing requests. */
    ProxyRelayService getProxyRelayService();

    /** Returns metadata service. */
    MetadataService getMetadataService();

    /** Returns admin service. */
    AdminService getAdminService();

    /** Returns lite subscription service. */
    LiteSubscriptionService getLiteSubscriptionService();
}
```

### 9.3 Proxy RequestPipeline

定义：Proxy remoting 和 gRPC 请求的 pipeline。

```java
/**
 * Proxy remoting request pipeline stage.
 */
public interface RequestPipeline {

    /** Executes this stage for a remoting request. */
    void execute(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context) throws Exception;

    /** Returns a composed pipeline that runs source before this stage. */
    default RequestPipeline pipe(RequestPipeline source);
}
```

```java
/**
 * Proxy gRPC request pipeline stage.
 */
public interface RequestPipeline {

    /** Executes this stage for a gRPC request. */
    void execute(ProxyContext context, Metadata headers, GeneratedMessageV3 request);

    /** Returns a composed pipeline that runs source before this stage. */
    default RequestPipeline pipe(RequestPipeline source);
}
```

### 9.4 GrpcServerBuilder

定义：Proxy gRPC server 构造扩展面，可添加 gRPC service 和 interceptor。

```java
/**
 * Builder for Proxy gRPC server.
 */
public class GrpcServerBuilder {

    /** Creates a builder for the configured gRPC server port and executor. */
    public static GrpcServerBuilder newBuilder(ThreadPoolExecutor executor, int port,
        TlsCertificateManager tlsCertificateManager);

    /** Sets graceful shutdown timeout. */
    public GrpcServerBuilder shutdownTime(long time, TimeUnit unit);

    /** Adds a bindable gRPC service. */
    public GrpcServerBuilder addService(BindableService service);

    /** Adds a generated gRPC service definition. */
    public GrpcServerBuilder addService(ServerServiceDefinition service);

    /** Appends a gRPC server interceptor. */
    public GrpcServerBuilder appendInterceptor(ServerInterceptor interceptor);

    /** Builds the gRPC server wrapper. */
    public GrpcServer build() throws Exception;

    /** Adds default Proxy interceptors. */
    public GrpcServerBuilder configInterceptor();
}
```

### 9.5 ProtocolHandler

定义：Proxy remoting 多协议识别和 pipeline 配置接口。

```java
/**
 * Handler that detects and configures one frontend protocol.
 */
public interface ProtocolHandler {

    /** Returns true when the incoming bytes match this protocol. */
    boolean match(ByteBuf msg);

    /** Configures the Netty pipeline for the matched protocol. */
    void config(ChannelHandlerContext ctx, ByteBuf msg);
}
```

### 9.6 TlsContextReloadListener

定义：Proxy TLS 证书和私钥变更后的 SSL context reload 监听器。

```java
/**
 * Listener notified when Proxy TLS context should be reloaded.
 */
public interface TlsContextReloadListener {

    /** Handles TLS context reload. */
    void onTlsContextReload();
}
```

### 9.7 QueueSelector

定义：Proxy 发送或接收路径选择目标队列的接口。

```java
/**
 * Selects an addressable message queue for a Proxy operation.
 */
public interface QueueSelector {

    /** Selects a queue from the topic route view. */
    AddressableMessageQueue select(ProxyContext ctx, MessageQueueView messageQueueView);
}
```

### 9.8 MessageQueuePriorityProvider

定义：Proxy 路由队列优先级提供器，数值越小优先级越高。

```java
/**
 * Provides priority values for message queues.
 *
 * @param <Q> message queue type
 */
@FunctionalInterface
public interface MessageQueuePriorityProvider<Q extends MessageQueue> {

    /** Returns the priority of the queue; smaller values are preferred. */
    int priorityOf(Q q);

    /** Groups queues by priority in ascending priority order. */
    static <Q extends MessageQueue> List<List<Q>> buildPriorityGroups(List<Q> queues,
        MessageQueuePriorityProvider<Q> provider);
}
```

### 9.9 MessageQueuePenalizer

定义：Proxy 路由队列惩罚器，分值越低越优先。

```java
/**
 * Computes a routing penalty for a message queue.
 *
 * @param <Q> message queue type
 */
@FunctionalInterface
public interface MessageQueuePenalizer<Q extends MessageQueue> {

    /** Returns the penalty of the queue; smaller values are preferred. */
    int penaltyOf(Q messageQueue);

    /** Sums penalties from all penalizers for a queue. */
    static <Q extends MessageQueue> int evaluatePenalty(Q messageQueue,
        List<MessageQueuePenalizer<Q>> penalizers);

    /** Selects the queue with the lowest penalty. */
    static <Q extends MessageQueue> Pair<Q, Integer> selectLeastPenalty(List<Q> queues,
        List<MessageQueuePenalizer<Q>> penalizers, AtomicInteger startIndex);

    /** Selects the best queue across priority groups and penalties. */
    static <Q extends MessageQueue> Pair<Q, Integer> selectLeastPenaltyWithPriority(
        List<List<Q>> queuesWithPriority, List<MessageQueuePenalizer<Q>> penalizers,
        AtomicInteger startIndex);
}
```

### 9.10 PopMessageResultFilter

定义：Proxy pop 消息结果过滤器，决定消息是否匹配、返回、进 DLQ 或继续留给客户端。

```java
/**
 * Filters messages returned by pop operations.
 */
public interface PopMessageResultFilter {

    /**
     * Result of filtering one popped message.
     */
    enum FilterResult {
        /** Send the message to DLQ. */
        TO_DLQ,
        /** Message does not match subscription data. */
        NO_MATCH,
        /** Message matches and can be returned. */
        MATCH,
        /** Return message to broker-side invisible state handling. */
        TO_RETURN
    }

    /** Filters one popped message. */
    FilterResult filterMessage(ProxyContext ctx, String consumerGroup,
        SubscriptionData subscriptionData, MessageExt messageExt);
}
```

### 9.11 TopicMessageTypeValidator

定义：Proxy 发送或召回时校验 topic 类型和消息类型是否匹配。

```java
/**
 * Validates whether an operation's message type matches the target topic type.
 */
public interface TopicMessageTypeValidator {

    /** Validates expected topic type against actual message type and throws on failure. */
    void validate(TopicMessageType expectedType, TopicMessageType actualType);
}
```

### 9.12 Proxy 服务层抽象接口

定义：Proxy 内部服务层抽象。它们不是独立的公开插件注册点，但可以作为替换 `ServiceManager` 或嵌入式 Proxy 时的服务接口边界。

```java
/**
 * Administrative service used by Proxy to create and inspect broker metadata.
 */
public interface AdminService {

    /** Returns whether the topic exists. */
    boolean topicExist(String topic);

    /** Creates a topic using a sample topic on the selected topic broker when needed. */
    boolean createTopicOnTopicBrokerIfNotExist(String createTopic, String sampleTopic,
        int wQueueNum, int rQueueNum, boolean examineTopic, int retryCheckCount);

    /** Creates a topic on selected brokers using current and sample broker data. */
    boolean createTopicOnBroker(String topic, int wQueueNum, int rQueueNum,
        List<BrokerData> curBrokerDataList, List<BrokerData> sampleBrokerDataList,
        boolean examineTopic, int retryCheckCount) throws Exception;
}
```

```java
/**
 * Metadata service used by Proxy processors and auth providers.
 */
public interface MetadataService {

    /** Returns the configured message type of a topic. */
    TopicMessageType getTopicMessageType(ProxyContext ctx, String topic);

    /** Returns subscription group configuration. */
    SubscriptionGroupConfig getSubscriptionGroupConfig(ProxyContext ctx, String group);

    /** Gets an authentication user by username. */
    CompletableFuture<User> getUser(ProxyContext ctx, String username);

    /** Gets an authorization ACL by subject. */
    CompletableFuture<Acl> getAcl(ProxyContext ctx, Subject subject);
}
```

```java
/**
 * Message operation service used by Proxy to access brokers.
 */
public interface MessageService {

    /** Sends messages to an addressable broker queue. */
    CompletableFuture<List<SendResult>> sendMessage(ProxyContext ctx,
        AddressableMessageQueue messageQueue, List<Message> msgList,
        SendMessageRequestHeader requestHeader, long timeoutMillis);

    /** Sends a consumed message back for retry or DLQ handling. */
    CompletableFuture<RemotingCommand> sendMessageBack(ProxyContext ctx, ReceiptHandle handle,
        String messageId, ConsumerSendMsgBackRequestHeader requestHeader, long timeoutMillis);

    /** Ends a transactional message with a one-way broker request. */
    CompletableFuture<Void> endTransactionOneway(ProxyContext ctx, String brokerName,
        EndTransactionRequestHeader requestHeader, long timeoutMillis);

    /** Pops messages from a broker queue. */
    CompletableFuture<PopResult> popMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        PopMessageRequestHeader requestHeader, long timeoutMillis);

    /** Pops lite messages from a broker queue. */
    CompletableFuture<PopResult> popLiteMessage(ProxyContext ctx,
        AddressableMessageQueue messageQueue, PopLiteMessageRequestHeader requestHeader,
        long timeoutMillis);

    /** Changes invisible time for a popped message. */
    CompletableFuture<AckResult> changeInvisibleTime(ProxyContext ctx, ReceiptHandle handle,
        String messageId, ChangeInvisibleTimeRequestHeader requestHeader, long timeoutMillis);

    /** Acknowledges one popped message. */
    CompletableFuture<AckResult> ackMessage(ProxyContext ctx, ReceiptHandle handle,
        String messageId, AckMessageRequestHeader requestHeader, long timeoutMillis);

    /** Acknowledges a batch of receipt handles. */
    CompletableFuture<AckResult> batchAckMessage(ProxyContext ctx,
        List<ReceiptHandleMessage> handleList, String consumerGroup, String topic,
        long timeoutMillis);

    /** Pulls messages from a broker queue. */
    CompletableFuture<PullResult> pullMessage(ProxyContext ctx,
        AddressableMessageQueue messageQueue, PullMessageRequestHeader requestHeader,
        long timeoutMillis);

    /** Queries consumer offset for a queue. */
    CompletableFuture<Long> queryConsumerOffset(ProxyContext ctx,
        AddressableMessageQueue messageQueue, QueryConsumerOffsetRequestHeader requestHeader,
        long timeoutMillis);

    /** Updates consumer offset synchronously from the caller's perspective. */
    CompletableFuture<Void> updateConsumerOffset(ProxyContext ctx,
        AddressableMessageQueue messageQueue, UpdateConsumerOffsetRequestHeader requestHeader,
        long timeoutMillis);

    /** Updates consumer offset asynchronously. */
    CompletableFuture<Void> updateConsumerOffsetAsync(ProxyContext ctx,
        AddressableMessageQueue messageQueue, UpdateConsumerOffsetRequestHeader requestHeader,
        long timeoutMillis);

    /** Locks a batch of message queues for orderly consumption. */
    CompletableFuture<Set<MessageQueue>> lockBatchMQ(ProxyContext ctx,
        AddressableMessageQueue messageQueue, LockBatchRequestBody requestBody,
        long timeoutMillis);

    /** Unlocks a batch of message queues for orderly consumption. */
    CompletableFuture<Void> unlockBatchMQ(ProxyContext ctx, AddressableMessageQueue messageQueue,
        UnlockBatchRequestBody requestBody, long timeoutMillis);

    /** Gets the max offset of a queue. */
    CompletableFuture<Long> getMaxOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        GetMaxOffsetRequestHeader requestHeader, long timeoutMillis);

    /** Gets the min offset of a queue. */
    CompletableFuture<Long> getMinOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        GetMinOffsetRequestHeader requestHeader, long timeoutMillis);

    /** Recalls a delayed message by broker name. */
    CompletableFuture<String> recallMessage(ProxyContext ctx, String brokerName,
        RecallMessageRequestHeader requestHeader, long timeoutMillis);

    /** Sends a generic remoting request to a broker. */
    CompletableFuture<RemotingCommand> request(ProxyContext ctx, String brokerName,
        RemotingCommand request, long timeoutMillis);

    /** Sends a generic one-way remoting request to a broker. */
    CompletableFuture<Void> requestOneway(ProxyContext ctx, String brokerName,
        RemotingCommand request, long timeoutMillis);
}
```

```java
/**
 * Transaction metadata service used by Proxy transaction processing.
 */
public interface TransactionService {

    /** Adds transaction topic subscriptions for a producer group. */
    void addTransactionSubscription(ProxyContext ctx, String group, List<String> topicList);

    /** Adds one transaction topic subscription for a producer group. */
    void addTransactionSubscription(ProxyContext ctx, String group, String topic);

    /** Replaces transaction topic subscriptions for a producer group. */
    void replaceTransactionSubscription(ProxyContext ctx, String group, List<String> topicList);

    /** Removes all transaction topic subscriptions for a producer group. */
    void unSubscribeAllTransactionTopic(ProxyContext ctx, String group);

    /** Adds transaction data indexed by broker address. */
    TransactionData addTransactionDataByBrokerAddr(ProxyContext ctx, String brokerAddr,
        String topic, String producerGroup, long tranStateTableOffset, long commitLogOffset,
        String transactionId, Message message);

    /** Adds transaction data indexed by broker name. */
    TransactionData addTransactionDataByBrokerName(ProxyContext ctx, String brokerName,
        String topic, String producerGroup, long tranStateTableOffset, long commitLogOffset,
        String transactionId, Message message);

    /** Builds end-transaction request data from stored transaction information. */
    EndTransactionRequestData genEndTransactionRequestHeader(ProxyContext ctx, String topic,
        String producerGroup, Integer commitOrRollback, boolean fromTransactionCheck,
        String msgId, String transactionId);

    /** Handles failed transaction-check delivery. */
    void onSendCheckTransactionStateFailed(ProxyContext context, String producerGroup,
        TransactionData transactionData);
}
```

```java
/**
 * Relay service for broker-facing Proxy requests that need response adaptation.
 */
public interface ProxyRelayService {

    /** Relays get-consumer-running-info to the target broker. */
    CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> processGetConsumerRunningInfo(
        ProxyContext context, RemotingCommand command, GetConsumerRunningInfoRequestHeader header);

    /** Relays consume-message-directly to the target broker. */
    CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> processConsumeMessageDirectly(
        ProxyContext context, RemotingCommand command,
        ConsumeMessageDirectlyResultRequestHeader header);

    /** Relays transaction-state check data toward the producer side. */
    RelayData<TransactionData, Void> processCheckTransactionState(ProxyContext context,
        RemotingCommand command, CheckTransactionStateRequestHeader header, MessageExt messageExt);
}
```

```java
/**
 * gRPC v2 activity interface exposed by the Proxy gRPC application.
 */
public interface GrpcMessagingActivity extends StartAndShutdown {

    /** Handles route query requests. */
    CompletableFuture<QueryRouteResponse> queryRoute(ProxyContext ctx, QueryRouteRequest request);

    /** Handles client heartbeat requests. */
    CompletableFuture<HeartbeatResponse> heartbeat(ProxyContext ctx, HeartbeatRequest request);

    /** Handles send-message requests. */
    CompletableFuture<SendMessageResponse> sendMessage(ProxyContext ctx, SendMessageRequest request);

    /** Handles assignment query requests. */
    CompletableFuture<QueryAssignmentResponse> queryAssignment(ProxyContext ctx,
        QueryAssignmentRequest request);

    /** Streams received messages to the response observer. */
    void receiveMessage(ProxyContext ctx, ReceiveMessageRequest request,
        StreamObserver<ReceiveMessageResponse> responseObserver);

    /** Handles ack-message requests. */
    CompletableFuture<AckMessageResponse> ackMessage(ProxyContext ctx, AckMessageRequest request);

    /** Handles forward-to-DLQ requests. */
    CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(
        ProxyContext ctx, ForwardMessageToDeadLetterQueueRequest request);

    /** Handles end-transaction requests. */
    CompletableFuture<EndTransactionResponse> endTransaction(ProxyContext ctx,
        EndTransactionRequest request);

    /** Handles client termination notifications. */
    CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(ProxyContext ctx,
        NotifyClientTerminationRequest request);

    /** Handles invisible-duration change requests. */
    CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(ProxyContext ctx,
        ChangeInvisibleDurationRequest request);

    /** Handles delayed-message recall requests. */
    CompletableFuture<RecallMessageResponse> recallMessage(ProxyContext ctx,
        RecallMessageRequest request);

    /** Handles lite subscription synchronization requests. */
    CompletableFuture<SyncLiteSubscriptionResponse> syncLiteSubscription(ProxyContext ctx,
        SyncLiteSubscriptionRequest request);

    /** Opens the telemetry bidirectional stream. */
    ContextStreamObserver<TelemetryCommand> telemetry(
        StreamObserver<TelemetryCommand> responseObserver);
}
```

## 10. 通用服务扩展点

### 10.1 FileWatchService.Listener

定义：文件内容 hash 变化监听器。当前 TLS 文件监听使用该扩展点。

```java
/**
 * Listener notified when watched files change.
 */
public interface Listener {

    /** Handles a changed watched file path. */
    void onChanged(String path);
}
```

### 10.2 StateEventListener

定义：通用状态事件监听器。

```java
/**
 * Listener for typed state events.
 *
 * @param <T> event type
 */
public interface StateEventListener<T> {

    /** Handles one state event. */
    void fireEvent(T event);
}
```

### 10.3 Handler

定义：通用责任链处理器。Auth 默认 provider 使用 `HandlerChain` 组合认证/授权 handler。

```java
/**
 * Handler in a typed responsibility chain.
 *
 * @param <T> input type
 * @param <R> result type
 */
public interface Handler<T, R> {

    /** Handles input and may delegate to the next handler in the chain. */
    R handle(T t, HandlerChain<T, R> chain);
}
```

### 10.4 RetryPolicy

定义：重试或续期延迟计算策略。

```java
/**
 * Computes the next delay duration for retry-like workflows.
 */
public interface RetryPolicy {

    /** Returns the next delay duration in milliseconds for the given reconsume count. */
    long nextDelayDuration(int reconsumeTimes);
}
```

### 10.5 Start / Shutdown / StartAndShutdown

定义：服务生命周期接口，Proxy service manager 和多个服务组件实现该组接口。

```java
/**
 * Component that can be started.
 */
public interface Start {

    /** Starts the component. */
    void start() throws Exception;
}
```

```java
/**
 * Component that can be shut down.
 */
public interface Shutdown {

    /** Shuts down the component. */
    void shutdown() throws Exception;
}
```

```java
/**
 * Component with start and shutdown lifecycle methods.
 */
public interface StartAndShutdown extends Start, Shutdown {

    /** Runs before shutdown when a component needs a pre-stop phase. */
    default void preShutdown() throws Exception {}
}
```

## 11. 当前未完整外部化的接口

以下接口或策略在源码中存在，但当前不是完整的外部插件机制：

- `FilterCheckHook`：接口存在，但主源码中未发现注册或调用路径。
- `BrokerAttachedPlugin`：插件生命周期调用存在，但当前 OSS 源码未提供自动发现或配置类名装载。
- `ColdCtrStrategy`：接口存在，但生产路径当前只在内置策略之间切换。
- `TieredStore MessageStoreFilter`：作为 TieredStore 内部 topic 黑名单过滤使用。
- `QueryAssignmentProcessor` 中的 `AllocateMessageQueueStrategy`：来自 client rebalance 策略，Broker 内部维护私有映射，没有公开注册方法。
- `CompressorFactory`：只维护固定内置压缩器映射，没有注册方法或配置项。

## 12. 源码定位索引

| 扩展点 | 源码文件 |
| --- | --- |
| `ServiceProvider` | `common/src/main/java/org/apache/rocketmq/common/utils/ServiceProvider.java` |
| `BrokerConfig.messageStorePlugIn` | `common/src/main/java/org/apache/rocketmq/common/BrokerConfig.java` |
| `MessageStoreFactory` | `store/src/main/java/org/apache/rocketmq/store/plugin/MessageStoreFactory.java` |
| `AbstractPluginMessageStore` | `store/src/main/java/org/apache/rocketmq/store/plugin/AbstractPluginMessageStore.java` |
| `MessageStorePluginContext` | `store/src/main/java/org/apache/rocketmq/store/plugin/MessageStorePluginContext.java` |
| `PutMessageHook` | `store/src/main/java/org/apache/rocketmq/store/hook/PutMessageHook.java` |
| `SendMessageBackHook` | `store/src/main/java/org/apache/rocketmq/store/hook/SendMessageBackHook.java` |
| `MessageArrivingListener` | `store/src/main/java/org/apache/rocketmq/store/MessageArrivingListener.java` |
| `HAService` | `store/src/main/java/org/apache/rocketmq/store/ha/HAService.java` |
| `MappedFile` | `store/src/main/java/org/apache/rocketmq/store/logfile/MappedFile.java` |
| `TransactionalMessageService` | `broker/src/main/java/org/apache/rocketmq/broker/transaction/TransactionalMessageService.java` |
| `AbstractTransactionalMessageCheckListener` | `broker/src/main/java/org/apache/rocketmq/broker/transaction/AbstractTransactionalMessageCheckListener.java` |
| `BrokerAttachedPlugin` | `broker/src/main/java/org/apache/rocketmq/broker/plugin/BrokerAttachedPlugin.java` |
| `PullMessageResultHandler` | `broker/src/main/java/org/apache/rocketmq/broker/plugin/PullMessageResultHandler.java` |
| `SendMessageHook` / `ConsumeMessageHook` | `broker/src/main/java/org/apache/rocketmq/broker/mqtrace/` |
| Auth 扩展接口 | `auth/src/main/java/org/apache/rocketmq/auth/` |
| TieredStore 扩展接口 | `tieredstore/src/main/java/org/apache/rocketmq/tieredstore/` |
| `FilterSpi` | `filter/src/main/java/org/apache/rocketmq/filter/FilterSpi.java` |
| `RPCHook` / `ChannelEventListener` | `remoting/src/main/java/org/apache/rocketmq/remoting/` |
| Broker `RequestPipeline` | `remoting/src/main/java/org/apache/rocketmq/remoting/pipeline/RequestPipeline.java` |
| Proxy remoting/gRPC pipeline | `proxy/src/main/java/org/apache/rocketmq/proxy/remoting/pipeline/RequestPipeline.java`, `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/pipeline/RequestPipeline.java` |
| Proxy routing扩展 | `proxy/src/main/java/org/apache/rocketmq/proxy/service/route/` |
| Controller 扩展接口 | `controller/src/main/java/org/apache/rocketmq/controller/` |
| `FileWatchService.Listener` | `srvutil/src/main/java/org/apache/rocketmq/srvutil/FileWatchService.java` |
