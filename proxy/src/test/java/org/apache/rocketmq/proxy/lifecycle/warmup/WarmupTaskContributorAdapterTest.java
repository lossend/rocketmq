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

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.apache.rocketmq.proxy.lifecycle.ReadinessContributor;
import org.apache.rocketmq.proxy.lifecycle.ReadinessResult;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;

public class WarmupTaskContributorAdapterTest {

    @Test
    @DisplayName("a successful task maps to a successful readiness result")
    public void successMapsThrough() {
        ReadinessContributor adapter = new WarmupTaskContributorAdapter(
            task("ok", FailureScope.LOCAL_FATAL, WarmupResult::success));

        ReadinessResult result = adapter.check().join();

        assertThat(result.isSuccess()).isTrue();
        assertThat(adapter.name()).isEqualTo("ok");
        assertThat(adapter.failureScope()).isEqualTo(FailureScope.LOCAL_FATAL);
    }

    @Test
    @DisplayName("a failing task maps to a failing readiness result carrying the detail")
    public void failureMapsThrough() {
        ReadinessContributor adapter = new WarmupTaskContributorAdapter(
            task("bad", FailureScope.SHARED_DEPENDENCY, () -> WarmupResult.failure("down")));

        ReadinessResult result = adapter.check().join();

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.detail()).isEqualTo("down");
        assertThat(adapter.failureScope()).isEqualTo(FailureScope.SHARED_DEPENDENCY);
    }

    private static WarmupTask task(String name, FailureScope scope, Supplier<WarmupResult> result) {
        return new WarmupTask() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public int priority() {
                return 0;
            }

            @Override
            public CompletableFuture<WarmupResult> warmup() {
                return CompletableFuture.completedFuture(result.get());
            }

            @Override
            public FailureScope failureScope() {
                return scope;
            }
        };
    }
}
