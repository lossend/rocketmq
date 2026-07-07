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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
 * <p>Standard registrations (no label) contribute nothing to the tracked set. All other
 * event types are ignored. The index is safe for concurrent access.
 */
public class TopicClientInfoIndex implements ConsumerIdsChangeListener {

    /**
     * {@code topic -> (logicalGroup -> set of online gray labels)}.
     * Inner sets are created via {@link ConcurrentHashMap#newKeySet()} for lock-free updates.
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Set<String>>> activeIsolatedLabelTable =
        new ConcurrentHashMap<>();

    /**
     * Handles a consumer-group change event. Only {@link ConsumerGroupEvent#CLIENT_REGISTER}
     * and {@link ConsumerGroupEvent#CLIENT_UNREGISTER} are acted upon; every other event is a no-op.
     *
     * <p>Expected {@code args} shape for the two handled events (see broker {@code ConsumerManager}):
     * {@code args[0]} is a {@code ClientChannelInfo} (unused here) and {@code args[1]} is a
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
                updateLabels(group, args, true);
                break;
            case CLIENT_UNREGISTER:
                updateLabels(group, args, false);
                break;
            default:
                break;
        }
    }

    /**
     * Adds or removes the gray label carried by {@code group} for each topic in {@code args[1]}.
     * No-ops when {@code group} carries no gray label or {@code args} is malformed.
     *
     * @param group the effective group; a label is derived via {@link TrafficLabel#parseLabel(String)}
     * @param args  event arguments; {@code args[1]} is expected to be a {@code Set<String>} of topics
     * @param add   {@code true} to add the label, {@code false} to remove it
     */
    @SuppressWarnings("unchecked")
    private void updateLabels(String group, Object[] args, boolean add) {
        String label = TrafficLabel.parseLabel(group);
        if (label == null) {
            return;
        }
        if (args == null || args.length < 2 || !(args[1] instanceof Set)) {
            return;
        }
        String logicalGroup = TrafficLabel.parseLogicalGroup(group);
        Set<?> topics = (Set<?>) args[1];
        for (Object topic : topics) {
            if (topic instanceof String) {
                if (add) {
                    addLabel((String) topic, logicalGroup, label);
                } else {
                    removeLabel((String) topic, logicalGroup, label);
                }
            }
        }
    }

    /**
     * Records {@code label} as online for {@code (topic, logicalGroup)}.
     *
     * @param topic        the subscribed topic
     * @param logicalGroup the origin (logical) consumer group
     * @param label        the gray label to add
     */
    private void addLabel(String topic, String logicalGroup, String label) {
        activeIsolatedLabelTable
            .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
            .computeIfAbsent(logicalGroup, k -> ConcurrentHashMap.newKeySet())
            .add(label);
    }

    /**
     * Removes {@code label} from the online set for {@code (topic, logicalGroup)}, pruning
     * now-empty inner maps to avoid unbounded growth.
     *
     * @param topic        the subscribed topic
     * @param logicalGroup the origin (logical) consumer group
     * @param label        the gray label to remove
     */
    private void removeLabel(String topic, String logicalGroup, String label) {
        ConcurrentHashMap<String, Set<String>> groupTable = activeIsolatedLabelTable.get(topic);
        if (groupTable == null) {
            return;
        }
        Set<String> labels = groupTable.get(logicalGroup);
        if (labels == null) {
            return;
        }
        labels.remove(label);
        if (labels.isEmpty()) {
            groupTable.remove(logicalGroup, Collections.emptySet());
        }
        if (groupTable.isEmpty()) {
            activeIsolatedLabelTable.remove(topic, new ConcurrentHashMap<String, Set<String>>());
        }
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
        ConcurrentHashMap<String, Set<String>> groupTable = activeIsolatedLabelTable.get(topic);
        if (groupTable == null) {
            return Collections.emptySet();
        }
        Set<String> labels = groupTable.get(logicalGroup);
        if (labels == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(new HashSet<>(labels));
    }

    /**
     * No-op; this index holds no resources requiring cleanup.
     */
    @Override
    public void shutdown() {
    }
}
