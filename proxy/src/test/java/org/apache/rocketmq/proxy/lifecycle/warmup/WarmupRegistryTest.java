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
import java.util.stream.Collectors;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class WarmupRegistryTest {

    @Test
    @DisplayName("tasks() returns entries sorted by priority, stable for equal priorities")
    public void tasksSortedByPriorityStable() {
        WarmupRegistry registry = new WarmupRegistry();
        registry.register(task("c", 50));
        registry.register(task("a", 10));
        registry.register(task("b1", 30));
        registry.register(task("b2", 30));

        List<String> order = registry.tasks().stream().map(WarmupTask::name).collect(Collectors.toList());

        // a(10) < b1(30) == b2(30) [insertion order] < c(50)
        assertThat(order).containsExactly("a", "b1", "b2", "c");
    }

    @Test
    @DisplayName("tasks() is a defensive copy that does not affect the registry")
    public void tasksIsDefensiveCopy() {
        WarmupRegistry registry = new WarmupRegistry();
        registry.register(task("a", 10));

        List<WarmupTask> first = registry.tasks();
        first.clear();

        assertThat(registry.tasks()).hasSize(1);
    }

    @Test
    @DisplayName("registering a duplicate task name fails fast")
    public void duplicateNameRejected() {
        WarmupRegistry registry = new WarmupRegistry();
        registry.register(task("dup", 10));

        assertThatThrownBy(() -> registry.register(task("dup", 20)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("dup");
    }

    private static WarmupTask task(String name, int priority) {
        return new WarmupTask() {
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
                return CompletableFuture.completedFuture(WarmupResult.success());
            }

            @Override
            public FailureScope failureScope() {
                return FailureScope.SHARED_DEPENDENCY;
            }
        };
    }
}
