# RocketMQ Producer RetryPolicy 机制分析

## 1. 客户端配置入口

### 唯一公开 API：`ProducerBuilder.setMaxAttempts(int)`

```java
Producer producer = provider.newProducerBuilder()
    .setClientConfiguration(clientConfig)
    .setTopics("YourTopic")
    .setMaxAttempts(5)  // 默认 3
    .build();
```

构造时内部固定使用"立即重试"策略：

```java
// ProducerImpl.java:116
ExponentialBackoffRetryPolicy retryPolicy =
    ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(maxAttempts);
// initialBackoff=0, maxBackoff=0, multiplier=1 — 写死，不可通过公开 API 修改
```

### rocketmq-spring 对应配置

| 方式 | 配置 |
|---|---|
| 全局（`application.yml`） | `rocketmq.producer.max-attempts: 5` |
| 多实例（注解） | `@ExtProducerResetConfiguration(maxAttempts = 5)` |

两种方式均只能配置 `maxAttempts`，backoff 参数无法从 Spring 层面设置。

---

## 2. 服务端下推机制

### 流程概览

```
Client 启动
  │
  ▼
ClientSessionImpl.syncSettings0()
  │  将本地 Settings（含 retryPolicy）序列化为 TelemetryCommand 发送给 Proxy
  ▼
Proxy: GrpcClientSettingsManager.mergeProducerData()
  │  用 ProxyConfig 覆盖 backoffPolicy 后下推响应
  ▼
ClientSessionImpl.onNext(TelemetryCommand{SETTINGS})
  │
  ▼
ClientImpl.onSettingsCommand()
  │  → this.getSettings().sync(settings)
  ▼
PublishingSettings.sync()
  │
  └─ retryPolicy = exist.inheritBackoff(backoffPolicy)
```

### `inheritBackoff` 的合并语义

| 字段 | 来源 | 说明 |
|---|---|---|
| `maxAttempts` | **客户端** | 由 `ProducerBuilder.setMaxAttempts()` 决定，服务端下推的值被忽略 |
| `initialBackoff` | **Proxy 下推** | 首次重试等待时间 |
| `maxBackoff` | **Proxy 下推** | 重试等待上限 |
| `backoffMultiplier` | **Proxy 下推** | 每次翻倍系数 |

```java
// ExponentialBackoffRetryPolicy.java
private RetryPolicy inheritBackoff(ExponentialBackoff backoff) {
    return new ExponentialBackoffRetryPolicy(
        maxAttempts,                                    // ← 保留客户端值
        Duration.ofNanos(...backoff.getInitial()),      // ← 来自 Proxy
        Duration.ofNanos(...backoff.getMax()),           // ← 来自 Proxy
        backoff.getMultiplier()                         // ← 来自 Proxy
    );
}
```

### Proxy 侧默认值（`ProxyConfig.java:151`）

| 配置字段 | 默认值 | 含义 |
|---|---|---|
| `grpcClientProducerMaxAttempts` | `3` | 下推的 maxAttempts（客户端不采用） |
| `grpcClientProducerBackoffInitialMillis` | `10 ms` | 首次重试等待 |
| `grpcClientProducerBackoffMaxMillis` | `1000 ms` | 重试等待上限 |
| `grpcClientProducerBackoffMultiplier` | `2` | 每次翻倍系数 |

**修改方式**：在 Proxy 配置文件（`rmq-proxy.json`）中覆盖以上字段，无需重启客户端，下次 Settings 握手时自动生效。

### 动态生效原理

`retryPolicy` 在 `Settings` 基类中声明为 `volatile`，Proxy 随时推送新 Settings 都能被正在发消息的线程立即看到。连接断开重连后，`renewRequestObserver()` 也会重新触发 `syncSettings0()`，确保配置同步。

---

## 3. 访问权限分析

直接调用 `settings.sync()` 模拟服务端下推**理论可行但实际被封死**：

| 层 | 限制 |
|---|---|
| `ProducerImpl` | `package-private` class，用户只能持有 `Producer` 接口 |
| `publishingSettings` | `protected` 字段 |
| `PublishingSettings.sync()` | `public`，但无法从外部获取实例 |

若强行通过反射修改，可直接写 `retryPolicy` 字段：

```java
// 不推荐用于生产，内部 API 随时可能变更
Field implField = producer.getClass().getDeclaredField("publishingSettings");
implField.setAccessible(true);
PublishingSettings ps = (PublishingSettings) implField.get(producer);

Field f = Settings.class.getDeclaredField("retryPolicy");
f.setAccessible(true);
f.set(ps, new ExponentialBackoffRetryPolicy(
    3,
    Duration.ofMillis(100),  // initialBackoff
    Duration.ofSeconds(3),   // maxBackoff
    2.0                      // multiplier
));
```

---

## 4. 各修改方式汇总

| 方式 | 可配置项 | 适用场景 |
|---|---|---|
| `ProducerBuilder.setMaxAttempts()` | `maxAttempts` | 标准用法，推荐 |
| `application.yml` | `maxAttempts` | Spring Boot 全局配置 |
| `@ExtProducerResetConfiguration` | `maxAttempts` | Spring Boot 多 Producer 实例 |
| Proxy `rmq-proxy.json` | `initialBackoff` / `maxBackoff` / `multiplier` | 运维侧统一调整，动态生效 |
| 反射修改 `retryPolicy` | 全部参数 | Hack，不推荐 |
