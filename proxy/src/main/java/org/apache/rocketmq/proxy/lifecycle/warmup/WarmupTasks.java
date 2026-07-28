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

package org.apache.rocketmq.proxy.lifecycle.warmup;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;

/**
 * Factory for the proxy's built-in {@link WarmupTask}s. Priorities are assigned so the
 * default evaluation order matches the previously hardcoded contributor list: local
 * listener/processor signals first, then the NameServer probe, then per-topic broker probes.
 */
public final class WarmupTasks {

    /** Default priorities; lower runs first. Broker tasks share one band, ordered by insertion. */
    public static final int PRIORITY_GRPC_LISTENER = 10;
    public static final int PRIORITY_REMOTING_LISTENER = 20;
    public static final int PRIORITY_PROCESSOR = 30;
    public static final int PRIORITY_NAMESERVER = 40;
    public static final int PRIORITY_BROKER = 50;

    private WarmupTasks() {
    }

    /**
     * A task backed by a synchronous boolean signal, for cheap non-blocking checks such as
     * "listener bound" or "processor started".
     *
     * @param name         stable task name
     * @param priority     evaluation order key
     * @param failureScope failure classification
     * @param signal       true when the signal is satisfied
     * @return the warmup task
     */
    public static WarmupTask supplied(String name, int priority, FailureScope failureScope,
        BooleanSupplier signal) {
        return new SuppliedWarmupTask(name, priority, failureScope, () -> signal.getAsBoolean()
            ? WarmupResult.success()
            : WarmupResult.failure(name + " not satisfied"));
    }

    /**
     * A task that actively probes NameServer reachability. Classified SHARED_DEPENDENCY so a
     * later blip is fail-open for {@code /ready}. The probe must be client-side time-bounded.
     *
     * @param priority evaluation order key
     * @param probe    true when NameServer answered within its client-side timeout
     * @return the warmup task
     */
    public static WarmupTask nameServerReachable(int priority, BooleanSupplier probe) {
        return new SuppliedWarmupTask("namesrv-reachable", priority, FailureScope.SHARED_DEPENDENCY,
            () -> probe.getAsBoolean()
                ? WarmupResult.success()
                : WarmupResult.failure("namesrv-reachable unreachable"));
    }

    /**
     * A task that prewarms the proxy-to-broker data path for one topic, passing as soon as at
     * least one broker for the topic answers. Classified SHARED_DEPENDENCY and lenient so a
     * partial broker outage cannot pin the pod in STARTING.
     *
     * @param topic            business topic to prewarm
     * @param priority         evaluation order key
     * @param brokerAddrLookup resolves the topic to broker addresses; time-bounded, empty on failure
     * @param brokerProbe      time-bounded per-broker reachability probe
     * @return the warmup task
     */
    public static WarmupTask brokerReachable(String topic, int priority,
        Function<String, List<String>> brokerAddrLookup, Predicate<String> brokerProbe) {
        String name = "broker-reachable[" + topic + "]";
        return new SuppliedWarmupTask(name, priority, FailureScope.SHARED_DEPENDENCY,
            () -> probeBrokers(name, topic, brokerAddrLookup, brokerProbe));
    }

    private static WarmupResult probeBrokers(String name, String topic,
        Function<String, List<String>> brokerAddrLookup, Predicate<String> brokerProbe) {
        List<String> brokerAddrs;
        try {
            brokerAddrs = brokerAddrLookup.apply(topic);
        } catch (Exception e) {
            return WarmupResult.failure(name + " route lookup failed: " + e.getMessage());
        }
        if (brokerAddrs == null || brokerAddrs.isEmpty()) {
            return WarmupResult.failure(name + " no broker in route");
        }
        for (String addr : brokerAddrs) {
            try {
                if (brokerProbe.test(addr)) {
                    return WarmupResult.success();
                }
            } catch (Exception e) {
                // try the next broker; a single unreachable broker must not fail warmup
            }
        }
        return WarmupResult.failure(name + " no reachable broker among " + brokerAddrs.size());
    }

    /** A {@link WarmupTask} evaluating a synchronous, exception-safe result supplier. */
    private static final class SuppliedWarmupTask implements WarmupTask {

        private final String name;
        private final int priority;
        private final FailureScope failureScope;
        private final Supplier<WarmupResult> evaluator;

        private SuppliedWarmupTask(String name, int priority, FailureScope failureScope,
            Supplier<WarmupResult> evaluator) {
            this.name = name;
            this.priority = priority;
            this.failureScope = failureScope;
            this.evaluator = evaluator;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public int priority() {
            return priority;
        }

        @Override
        public CompletableFuture<WarmupResult> warmup() {
            try {
                return CompletableFuture.completedFuture(evaluator.get());
            } catch (Exception e) {
                return CompletableFuture.completedFuture(WarmupResult.failure(name + ": " + e.getMessage()));
            }
        }

        @Override
        public FailureScope failureScope() {
            return failureScope;
        }
    }
}
