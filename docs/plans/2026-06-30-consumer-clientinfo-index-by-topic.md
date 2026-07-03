# Consumer ClientInfo Indexing by Topic

## Background

In RocketMQ Proxy cluster mode, we need to retrieve all `ClientChannelInfo` entries under a given `consumerGroup + topic`.

### Limitations of the Existing Framework Data Structures

Inside `ConsumerGroupInfo`, there are two independent maps:

```
ConsumerGroupInfo (group level)
  ├─ channelInfoTable:   Map<Channel, ClientChannelInfo>   // connection dimension, no topic field
  └─ subscriptionTable:  Map<Topic,   SubscriptionData>    // subscription dimension, no clientId field
```

There is **no linking field** between these two tables, so it is impossible to navigate directly from a topic to `ClientChannelInfo`.

Although `ConsumerManager` maintains a reverse index, `topicGroupTable: Map<topic, Set<group>>`, it can only find groups by topic. From there, it can only return **all clients in the group**, not clients filtered by topic.

### Why Filtering by Topic Within the Same Group Does Not Work

The cleanup logic in `ConsumerGroupInfo.updateSubscription()` removes any topic that is not present in the current heartbeat `subList` from `subscriptionTable`. If different clients in the same group subscribe to different topics, heartbeats will continuously overwrite and flap the subscription state.

RocketMQ enforces the rule that **all instances in the same ConsumerGroup must subscribe to the exact same set of topics**. Therefore, in the correct usage model, "all clients in the group" is effectively equal to "all clients under the topic", and the framework does not provide a more fine-grained query.

---

## Solution: Build a Custom `topic -> clientInfo` Index via `ConsumerIdsChangeListener`

### How It Works

When a new channel is registered, `ConsumerManager.registerConsumer()` triggers a `CLIENT_REGISTER` event. The event args include both `ClientChannelInfo` and the subscribed topic set, which naturally provides the `client <-> topic` relationship:

```java
// ConsumerManager.java:252
callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_REGISTER, group,
    clientChannelInfo,
    subList.stream().map(SubscriptionData::getTopic).collect(Collectors.toSet()));
// args[0] = ClientChannelInfo
// args[1] = Set<String> (topic set)
```

### Registration Entry Point

The `MessagingProcessor` interface provides an official registration method:

```java
// MessagingProcessor.java
void registerConsumerListener(ConsumerIdsChangeListener consumerIdsChangeListener);

// DefaultMessagingProcessor.java:362 implementation
@Override
public void registerConsumerListener(ConsumerIdsChangeListener listener) {
    this.clientProcessor.registerConsumerIdsChangeListener(listener);
    // -> serviceManager.getConsumerManager().appendConsumerIdsChangeListener(listener)
}
```

**Note**: RocketMQ Proxy does not have an SPI-based auto-discovery mechanism. The listener must be registered explicitly before `messagingProcessor.start()`.

### Implementation

```java
public class TopicClientInfoIndex implements ConsumerIdsChangeListener {

    // Map<topic, Map<group, Set<clientId>>>
    private final ConcurrentMap<String, ConcurrentMap<String, Set<String>>> index =
        new ConcurrentHashMap<>();

    @Override
    public void handle(ConsumerGroupEvent event, String group, Object... args) {
        switch (event) {
            case CLIENT_REGISTER: {
                ClientChannelInfo info = (ClientChannelInfo) args[0];
                @SuppressWarnings("unchecked")
                Set<String> topics = (Set<String>) args[1];
                for (String topic : topics) {
                    index.computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                         .computeIfAbsent(group, k -> ConcurrentHashMap.newKeySet())
                         .add(info.getClientId());
                }
                break;
            }
            case CLIENT_UNREGISTER: {
                ClientChannelInfo info = (ClientChannelInfo) args[0];
                @SuppressWarnings("unchecked")
                Set<String> topics = (Set<String>) args[1];
                for (String topic : topics) {
                    Map<String, Set<String>> groupMap = index.get(topic);
                    if (groupMap != null) {
                        Set<String> clients = groupMap.get(group);
                        if (clients != null) {
                            clients.remove(info.getClientId());
                        }
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    @Override
    public void shutdown() {}

    /**
     * Query all clientIds under the given group + topic.
     */
    public Set<String> getClientIds(String topic, String group) {
        Map<String, Set<String>> groupMap = index.get(topic);
        if (groupMap == null) return Collections.emptySet();
        Set<String> clients = groupMap.get(group);
        return clients != null ? Collections.unmodifiableSet(clients) : Collections.emptySet();
    }

    /**
     * Query clientIds for all groups under a topic (cross-group).
     */
    public Map<String, Set<String>> getClientIdsByTopic(String topic) {
        Map<String, Set<String>> groupMap = index.get(topic);
        return groupMap != null ? Collections.unmodifiableMap(groupMap) : Collections.emptyMap();
    }
}
```

### Register During Startup

```java
// In ProxyStartup or a custom bootstrap class
DefaultMessagingProcessor messagingProcessor = DefaultMessagingProcessor.createForClusterMode();

TopicClientInfoIndex topicClientInfoIndex = new TopicClientInfoIndex();
messagingProcessor.registerConsumerListener(topicClientInfoIndex);  // register before start()

messagingProcessor.start();
```

---

## Full Call Chain

```
Consumer heartbeat / registration
  └─ ClusterConsumerManager.registerConsumer()       // proxy/service/client/ClusterConsumerManager.java:46
       ├─ heartbeatSyncer.onConsumerRegister()        // broadcast to other Proxy nodes
       └─ super.registerConsumer()                    // broker/client/ConsumerManager.java:227
            ├─ consumerGroupInfo.updateChannel()      // update channelInfoTable
            ├─ consumerGroupInfo.updateSubscription() // update subscriptionTable
            └─ callConsumerIdsChangeListener(CLIENT_REGISTER, group, clientInfo, topics)
                 └─ TopicClientInfoIndex.handle()     // write to the custom index
```

---

## Data Consistency Notes

| Scenario | Handling |
|------|------|
| Client unregisters normally | `CLIENT_UNREGISTER` is triggered and removes the entry from the index |
| Channel closes unexpectedly (network interruption) | `doChannelCloseEvent()` -> `CLIENT_UNREGISTER`, which also performs cleanup |
| Subscription changes within the same group (heartbeat update) | `CLIENT_REGISTER` overwrites incrementally; old topics are cleaned up on `UNREGISTER`; if the subscription shrinks, stale entries depend on the next `UNREGISTER` for cleanup |
| `HeartbeatSyncer` syncs remote channels | When a remote channel is registered, `isNotifyConsumerIdsChangedEnable=false`, but it still triggers `REGISTER` rather than `CLIENT_REGISTER`; the custom index is not affected |

---

## Alternative Approaches

| Approach | Description | Drawback |
|------|------|------|
| `topicGroupTable` + `getChannelInfoTable()` | Native framework approach: O(1) lookup to get groups, then fetch all clients in the group | Cannot filter by topic; all clients in the same group are returned |
| Custom `ConsumerIdsChangeListener` index | Event-driven and accurately maintains the `topic -> clientId` mapping | Requires manual registration and some incremental maintenance logic |
| Group isolation | Use a separate consumer group per topic to eliminate the issue entirely | Changes the consumption model and affects load-balancing semantics |
