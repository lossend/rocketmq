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
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.apache.rocketmq.proxy.lifecycle.ReadinessResult;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ReadinessProbeRegistryTest {

    @Test
    @DisplayName("probes() returns every registered probe")
    public void probesReturnsAllRegistered() {
        ReadinessProbeRegistry registry = new ReadinessProbeRegistry();
        registry.register(probe("a"));
        registry.register(probe("b"));

        List<String> names = registry.probes().stream().map(ReadinessProbe::name)
            .collect(java.util.stream.Collectors.toList());

        assertThat(names).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    @DisplayName("probes() is a defensive copy that does not affect the registry")
    public void probesIsDefensiveCopy() {
        ReadinessProbeRegistry registry = new ReadinessProbeRegistry();
        registry.register(probe("a"));

        List<ReadinessProbe> first = registry.probes();
        first.clear();

        assertThat(registry.probes()).hasSize(1);
    }

    @Test
    @DisplayName("registering a duplicate probe name fails fast")
    public void duplicateNameRejected() {
        ReadinessProbeRegistry registry = new ReadinessProbeRegistry();
        registry.register(probe("dup"));

        assertThatThrownBy(() -> registry.register(probe("dup")))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("dup");
    }

    private static ReadinessProbe probe(String name) {
        return new ReadinessProbe() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public CompletableFuture<ReadinessResult> check() {
                return CompletableFuture.completedFuture(ReadinessResult.success());
            }

            @Override
            public FailureScope failureScope() {
                return FailureScope.SHARED_DEPENDENCY;
            }
        };
    }
}
