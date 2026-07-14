/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import io.netty.channel.Channel;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;

/**
 * Tracks which isolated (gray) traffic labels are currently online per
 * {@code (topic, logicalGroup)}, driven by broker consumer-registration events.
 *
 * <p>Gray consumers register at the broker under a rewritten group of the form
 * {@code originGroup%label}; standard consumers register under the plain
 * {@code originGroup}. This listener observes {@link ConsumerGroupEvent#CLIENT_REGISTER}
 * and {@link ConsumerGroupEvent#CLIENT_UNREGISTER} events, derives the
 * {@code (logicalGroup, label)} pair from the effective group, and maintains the set
 * of live gray labels so the standard receive branch can dynamically exclude only the
 * labels that are actually online.
 *
 * <p>A label is considered online as long as at least one {@link ClientChannelInfo} is
 * registered for it. Multiple clients may share the same label; the label is only removed
 * when the last client disconnects.
 *
 * <p>Standard registrations (no label) contribute nothing to the tracked set. All other
 * event types are ignored. The index is safe for concurrent access.
 */
public class TopicClientInfoIndex implements ConsumerIdsChangeListener {

    /**
     * {@code topic -> (logicalGroup -> (grayLabel -> (channel -> ClientChannelInfo)))}.
     * Keyed on {@link Channel} rather than {@link ClientChannelInfo} because
     * {@code ClientChannelInfo.hashCode()} includes mutable {@code lastUpdateTimestamp},
     * making it unsafe as a map key.
     */
    private final ConcurrentHashMap<String,
        ConcurrentHashMap<String,
            ConcurrentHashMap<String,
                ConcurrentHashMap<Channel, ClientChannelInfo>>>> activeIsolatedLabelTable =
        new ConcurrentHashMap<>();

    /**
     * Handles a consumer-group change event. Only {@link ConsumerGroupEvent#CLIENT_REGISTER}
     * and {@link ConsumerGroupEvent#CLIENT_UNREGISTER} are acted upon; every other event is a no-op.
     *
     * <p>Expected {@code args} shape for the two handled events (see broker {@code ConsumerManager}):
     * {@code args[0]} is a {@code ClientChannelInfo} and {@code args[1]} is a
     * {@code Set<String>} of subscribed topics. Malformed {@code args} (null, too short, or wrong
     * element types) are silently ignored rather than throwing.
     *
     * @param event the change event; may be {@code null}
     * @param group the broker-side effective group, e.g. {@code "G"} or {@code "G%gray1"}
     * @param args  event-specific arguments as documented above
     */
    @Override
    public void handle(ConsumerGroupEvent event, String group, Object... args) {
        if (event == null) {
            return;
        }
        switch (event) {
            case CLIENT_REGISTER:
                updateClients(group, args, true);
                break;
            case CLIENT_UNREGISTER:
                updateClients(group, args, false);
                break;
            default:
                break;
        }
    }

    /**
     * Adds or removes the client carried by {@code args[0]} for each topic in {@code args[1]}.
     * No-ops when {@code group} carries no gray label, {@code args} is malformed, or the
     * client's channel is {@code null}.
     *
     * @param group the effective group; a label is derived via {@link TrafficLabel#parseLabel(String)}
     * @param args  event arguments; {@code args[0]} is {@code ClientChannelInfo}, {@code args[1]} is {@code Set<String>} of topics
     * @param add   {@code true} to register the client, {@code false} to unregister it
     */
    @SuppressWarnings("unchecked")
    private void updateClients(String group, Object[] args, boolean add) {
        String label = TrafficLabel.parseLabel(group);
        if (label == null) {
            return;
        }
        if (args == null || args.length < 2
            || !(args[0] instanceof ClientChannelInfo)
            || !(args[1] instanceof Set)) {
            return;
        }
        ClientChannelInfo clientChannelInfo = (ClientChannelInfo) args[0];
        if (clientChannelInfo.getChannel() == null) {
            return;
        }
        String logicalGroup = TrafficLabel.parseLogicalGroup(group);
        Set<?> topics = (Set<?>) args[1];
        for (Object topic : topics) {
            if (topic instanceof String) {
                if (add) {
                    addClient((String) topic, logicalGroup, label, clientChannelInfo);
                } else {
                    removeClient((String) topic, logicalGroup, label, clientChannelInfo);
                }
            }
        }
    }

    private void addClient(String topic, String logicalGroup, String label, ClientChannelInfo client) {
        activeIsolatedLabelTable
            .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
            .computeIfAbsent(logicalGroup, k -> new ConcurrentHashMap<>())
            .computeIfAbsent(label, k -> new ConcurrentHashMap<>())
            .put(client.getChannel(), client);
    }

    private void removeClient(String topic, String logicalGroup, String label, ClientChannelInfo client) {
        ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<Channel, ClientChannelInfo>>> groupTable =
            activeIsolatedLabelTable.get(topic);
        if (groupTable == null) {
            return;
        }
        ConcurrentHashMap<String, ConcurrentHashMap<Channel, ClientChannelInfo>> labelTable =
            groupTable.get(logicalGroup);
        if (labelTable == null) {
            return;
        }
        ConcurrentHashMap<Channel, ClientChannelInfo> clients = labelTable.get(label);
        if (clients == null) {
            return;
        }
        clients.remove(client.getChannel());
        // computeIfPresent is atomic: removes the entry only if the map is still empty at
        // the moment of the check, preventing a race where passing the same reference via
        // remove(key, value) would always succeed even if a concurrent add just populated it.
        labelTable.computeIfPresent(label, (k, v) -> v.isEmpty() ? null : v);
        groupTable.computeIfPresent(logicalGroup, (k, v) -> v.isEmpty() ? null : v);
        activeIsolatedLabelTable.computeIfPresent(topic, (k, v) -> v.isEmpty() ? null : v);
    }

    /**
     * Returns an unmodifiable snapshot of the gray labels currently online for
     * {@code (topic, logicalGroup)}, optionally excluding one label.
     *
     * @param topic        the topic to look up
     * @param logicalGroup the origin (logical) consumer group to look up
     * @param excludeLabel label to exclude from the result; {@code null} to include all
     * @return an unmodifiable set of online gray labels; never {@code null}
     */
    public Set<String> getActiveIsolatedLabels(String topic, String logicalGroup, String excludeLabel) {
        ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<Channel, ClientChannelInfo>>> groupTable =
            activeIsolatedLabelTable.get(topic);
        if (groupTable == null) {
            return Collections.emptySet();
        }
        ConcurrentHashMap<String, ConcurrentHashMap<Channel, ClientChannelInfo>> labelTable =
            groupTable.get(logicalGroup);
        if (labelTable == null) {
            return Collections.emptySet();
        }
        Set<String> result = new HashSet<>();
        for (ConcurrentHashMap.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry : labelTable.entrySet()) {
            if (!entry.getValue().isEmpty() && !entry.getKey().equals(excludeLabel)) {
                result.add(entry.getKey());
            }
        }
        return Collections.unmodifiableSet(result);
    }

    /**
     * Returns an unmodifiable snapshot of the gray labels currently online for
     * {@code (topic, logicalGroup)}.
     *
     * @param topic        the topic to look up
     * @param logicalGroup the origin (logical) consumer group to look up
     * @return an unmodifiable set of online gray labels; never {@code null}, empty when nothing is tracked
     */
    public Set<String> getActiveIsolatedLabels(String topic, String logicalGroup) {
        return getActiveIsolatedLabels(topic, logicalGroup, null);
    }

    /**
     * No-op; this index holds no resources requiring cleanup.
     */
    @Override
    public void shutdown() {
    }
}
