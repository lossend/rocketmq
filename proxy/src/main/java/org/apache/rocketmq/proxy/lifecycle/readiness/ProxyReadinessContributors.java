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

package org.apache.rocketmq.proxy.lifecycle.readiness;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.apache.rocketmq.proxy.lifecycle.ReadinessContributor;

/**
 * Assembles the core-subset warmup contributors for the proxy graceful lifecycle.
 * The listener and processor signals are local (LOCAL_FATAL): once started they do
 * not un-start, so they mainly classify failure scope and serve as extension points.
 * The NameServer reachability probe is the only genuinely asynchronous signal and is
 * classified SHARED_DEPENDENCY so a later blip is fail-open for {@code /ready}.
 */
public final class ProxyReadinessContributors {

    private ProxyReadinessContributors() {
    }

    /**
     * Builds the ordered core-subset contributor list.
     *
     * @param grpcListenerBound     true once the gRPC business listener has bound
     * @param remotingListenerBound true once the Remoting business listener has bound
     * @param processorStarted      true once the MessagingProcessor has started
     * @param nameServerProbe       client-side time-bounded NameServer reachability probe
     * @return contributors evaluated by the warmup barrier, in check order
     */
    public static List<ReadinessContributor> coreSubset(BooleanSupplier grpcListenerBound,
        BooleanSupplier remotingListenerBound, BooleanSupplier processorStarted,
        BooleanSupplier nameServerProbe) {
        return coreSubset(grpcListenerBound, remotingListenerBound, processorStarted, nameServerProbe,
            Collections.emptyList(), null, null);
    }

    /**
     * Builds the core-subset contributors plus one broker-reachability contributor per
     * warmup topic. When {@code warmupTopics} is empty the result matches
     * {@link #coreSubset(BooleanSupplier, BooleanSupplier, BooleanSupplier, BooleanSupplier)},
     * so warmup stays at the NameServer probe and READY is not held for broker prewarm.
     *
     * @param grpcListenerBound     true once the gRPC business listener has bound
     * @param remotingListenerBound true once the Remoting business listener has bound
     * @param processorStarted      true once the MessagingProcessor has started
     * @param nameServerProbe       client-side time-bounded NameServer reachability probe
     * @param warmupTopics          topics whose broker data path is prewarmed; may be empty
     * @param brokerAddrLookup      resolves a topic to its broker addresses; required when
     *                              {@code warmupTopics} is non-empty
     * @param brokerProbe           lightweight time-bounded per-broker reachability probe;
     *                              required when {@code warmupTopics} is non-empty
     * @return contributors evaluated by the warmup barrier, in check order
     */
    public static List<ReadinessContributor> coreSubset(BooleanSupplier grpcListenerBound,
        BooleanSupplier remotingListenerBound, BooleanSupplier processorStarted,
        BooleanSupplier nameServerProbe, List<String> warmupTopics,
        Function<String, List<String>> brokerAddrLookup, Predicate<String> brokerProbe) {
        List<ReadinessContributor> contributors = new ArrayList<>();
        contributors.add(new SuppliedContributor("grpc-listener-bound", FailureScope.LOCAL_FATAL, grpcListenerBound));
        contributors.add(new SuppliedContributor("remoting-listener-bound", FailureScope.LOCAL_FATAL, remotingListenerBound));
        contributors.add(new SuppliedContributor("messaging-processor-started", FailureScope.LOCAL_FATAL, processorStarted));
        contributors.add(new NameServerReachableContributor(nameServerProbe));
        for (String topic : warmupTopics) {
            contributors.add(new BrokerReachableContributor(topic, brokerAddrLookup, brokerProbe));
        }
        return contributors;
    }
}
