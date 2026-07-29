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
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.apache.rocketmq.proxy.lifecycle.ReadinessResult;

/**
 * Factory for the proxy's built-in {@link ReadinessProbe}s: live runtime health checks that
 * back {@code /ready-for-traffic}, re-evaluated on every query. These are the same kind of
 * shared-dependency signal as the startup {@code WarmupTask}s (and typically wrap the same
 * underlying probe client), but registered separately since they run forever, not once.
 */
public final class ReadinessProbes {

    private ReadinessProbes() {
    }

    /**
     * A probe that actively checks NameServer reachability. Classified SHARED_DEPENDENCY so a
     * blip is fail-open for {@code /ready} and only fails {@code /ready-for-traffic} once the
     * consecutive-failure threshold is reached. The probe must be client-side time-bounded.
     *
     * @param probe true when NameServer answered within its client-side timeout
     * @return the readiness probe
     */
    public static ReadinessProbe nameServerReachable(BooleanSupplier probe) {
        return new SuppliedReadinessProbe("namesrv-reachable", FailureScope.SHARED_DEPENDENCY,
            () -> probe.getAsBoolean()
                ? ReadinessResult.success()
                : ReadinessResult.failure("namesrv-reachable unreachable"));
    }

    /**
     * A probe that checks the proxy-to-broker data path for one topic, passing as soon as at
     * least one broker for the topic answers. Classified SHARED_DEPENDENCY and lenient so a
     * partial broker outage does not fail {@code /ready-for-traffic} on its own.
     *
     * @param topic            business topic being probed
     * @param brokerAddrLookup resolves the topic to broker addresses; time-bounded, empty on failure
     * @param brokerProbe      time-bounded per-broker reachability probe
     * @return the readiness probe
     */
    public static ReadinessProbe brokerReachable(String topic,
        Function<String, List<String>> brokerAddrLookup, Predicate<String> brokerProbe) {
        String name = "broker-reachable[" + topic + "]";
        return new SuppliedReadinessProbe(name, FailureScope.SHARED_DEPENDENCY,
            () -> probeBrokers(name, topic, brokerAddrLookup, brokerProbe));
    }

    private static ReadinessResult probeBrokers(String name, String topic,
        Function<String, List<String>> brokerAddrLookup, Predicate<String> brokerProbe) {
        List<String> brokerAddrs;
        try {
            brokerAddrs = brokerAddrLookup.apply(topic);
        } catch (Exception e) {
            return ReadinessResult.failure(name + " route lookup failed: " + e.getMessage());
        }
        if (brokerAddrs == null || brokerAddrs.isEmpty()) {
            return ReadinessResult.failure(name + " no broker in route");
        }
        for (String addr : brokerAddrs) {
            try {
                if (brokerProbe.test(addr)) {
                    return ReadinessResult.success();
                }
            } catch (Exception e) {
                // try the next broker; a single unreachable broker must not fail the probe
            }
        }
        return ReadinessResult.failure(name + " no reachable broker among " + brokerAddrs.size());
    }

    /** A {@link ReadinessProbe} evaluating a synchronous, exception-safe result supplier. */
    private static final class SuppliedReadinessProbe implements ReadinessProbe {

        private final String name;
        private final FailureScope failureScope;
        private final Supplier<ReadinessResult> evaluator;

        private SuppliedReadinessProbe(String name, FailureScope failureScope,
            Supplier<ReadinessResult> evaluator) {
            this.name = name;
            this.failureScope = failureScope;
            this.evaluator = evaluator;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public CompletableFuture<ReadinessResult> check() {
            try {
                return CompletableFuture.completedFuture(evaluator.get());
            } catch (Exception e) {
                return CompletableFuture.completedFuture(ReadinessResult.failure(name + ": " + e.getMessage()));
            }
        }

        @Override
        public FailureScope failureScope() {
            return failureScope;
        }
    }
}
