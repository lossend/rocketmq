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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.apache.rocketmq.proxy.lifecycle.ReadinessContributor;
import org.apache.rocketmq.proxy.lifecycle.ReadinessResult;

/**
 * Prewarms the proxy-to-broker data path for one topic. It resolves the topic's
 * broker addresses (from NameServer) and probes them with a lightweight RPC,
 * succeeding as soon as at least one broker answers. This proves the data path is
 * usable and populates the route cache so the first business request is not cold.
 *
 * <p>Passing requires only {@code >= 1} reachable broker for the topic, deliberately
 * lenient so a partial broker outage cannot pin the pod in STARTING. The signal is
 * {@link FailureScope#SHARED_DEPENDENCY}: a later blip is fail-open for {@code /ready}
 * but fail-close for {@code /ready-for-traffic}.
 */
public final class BrokerReachableContributor implements ReadinessContributor {

    private final String topic;
    private final Function<String, List<String>> brokerAddrLookup;
    private final Predicate<String> brokerProbe;

    /**
     * Creates the contributor.
     *
     * @param topic            business topic whose route/broker path is prewarmed
     * @param brokerAddrLookup resolves the topic to its broker addresses; must be
     *                         client-side time-bounded and return empty on failure
     * @param brokerProbe      lightweight, time-bounded reachability probe for one
     *                         broker address; true when the broker answered
     */
    public BrokerReachableContributor(String topic, Function<String, List<String>> brokerAddrLookup,
        Predicate<String> brokerProbe) {
        this.topic = topic;
        this.brokerAddrLookup = brokerAddrLookup;
        this.brokerProbe = brokerProbe;
    }

    @Override
    public String name() {
        return "broker-reachable[" + topic + "]";
    }

    @Override
    public CompletableFuture<ReadinessResult> check() {
        return CompletableFuture.completedFuture(evaluate());
    }

    private ReadinessResult evaluate() {
        List<String> brokerAddrs;
        try {
            brokerAddrs = brokerAddrLookup.apply(topic);
        } catch (Exception e) {
            return ReadinessResult.failure(name() + " route lookup failed: " + e.getMessage());
        }
        if (brokerAddrs == null || brokerAddrs.isEmpty()) {
            return ReadinessResult.failure(name() + " no broker in route");
        }
        for (String addr : brokerAddrs) {
            try {
                if (brokerProbe.test(addr)) {
                    return ReadinessResult.success();
                }
            } catch (Exception e) {
                // try the next broker; a single unreachable broker must not fail warmup
            }
        }
        return ReadinessResult.failure(name() + " no reachable broker among " + brokerAddrs.size());
    }

    @Override
    public FailureScope failureScope() {
        return FailureScope.SHARED_DEPENDENCY;
    }
}
