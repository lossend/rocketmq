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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.apache.rocketmq.proxy.lifecycle.ReadinessResult;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerReachableContributorTest {

    private static final Function<String, List<String>> TWO_BROKERS =
        topic -> Arrays.asList("broker-a:10911", "broker-b:10911");

    @Test
    @DisplayName("passes as soon as one broker for the topic is reachable")
    public void oneReachableBrokerPasses() {
        Predicate<String> onlyBReachable = addr -> addr.startsWith("broker-b");
        BrokerReachableContributor contributor =
            new BrokerReachableContributor("TopicX", TWO_BROKERS, onlyBReachable);

        ReadinessResult result = contributor.check().join();

        assertThat(result.isSuccess()).isTrue();
        assertThat(contributor.failureScope()).isEqualTo(FailureScope.SHARED_DEPENDENCY);
    }

    @Test
    @DisplayName("fails when the topic route resolves to no broker")
    public void emptyRouteFails() {
        BrokerReachableContributor contributor =
            new BrokerReachableContributor("TopicX", topic -> Collections.emptyList(), addr -> true);

        ReadinessResult result = contributor.check().join();

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.detail()).contains("no broker in route");
    }

    @Test
    @DisplayName("fails when every broker for the topic is unreachable")
    public void allBrokersUnreachableFails() {
        BrokerReachableContributor contributor =
            new BrokerReachableContributor("TopicX", TWO_BROKERS, addr -> false);

        ReadinessResult result = contributor.check().join();

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.detail()).contains("no reachable broker");
    }

    @Test
    @DisplayName("a broker probe that throws is treated as unreachable, not propagated")
    public void throwingProbeIsCaught() {
        Predicate<String> thrower = addr -> {
            throw new IllegalStateException("connection refused");
        };
        BrokerReachableContributor contributor =
            new BrokerReachableContributor("TopicX", TWO_BROKERS, thrower);

        ReadinessResult result = contributor.check().join();

        assertThat(result.isSuccess()).isFalse();
    }

    @Test
    @DisplayName("a route lookup that throws fails the contributor without propagating")
    public void throwingRouteLookupIsCaught() {
        Function<String, List<String>> thrower = topic -> {
            throw new IllegalStateException("namesrv down");
        };
        BrokerReachableContributor contributor =
            new BrokerReachableContributor("TopicX", thrower, addr -> true);

        ReadinessResult result = contributor.check().join();

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.detail()).contains("route lookup failed");
    }
}
