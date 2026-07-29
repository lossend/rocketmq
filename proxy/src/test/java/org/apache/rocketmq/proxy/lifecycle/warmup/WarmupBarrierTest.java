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

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;

public class WarmupBarrierTest {

    private static WarmupTask task(String name, FailureScope scope, AtomicReference<WarmupResult> holder) {
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
                return CompletableFuture.completedFuture(holder.get());
            }

            @Override
            public FailureScope failureScope() {
                return scope;
            }
        };
    }

    @Test
    @DisplayName("warmup does not complete until every task succeeds once")
    public void warmupRequiresAll() {
        AtomicReference<WarmupResult> a = new AtomicReference<>(WarmupResult.failure("cold"));
        AtomicReference<WarmupResult> b = new AtomicReference<>(WarmupResult.success());
        WarmupBarrier barrier = new WarmupBarrier(Arrays.asList(
            task("a", FailureScope.SHARED_DEPENDENCY, a),
            task("b", FailureScope.LOCAL_FATAL, b)));

        assertThat(barrier.tryCompleteWarmup()).isFalse();
        assertThat(barrier.hasCompletedWarmup()).isFalse();
        a.set(WarmupResult.success());
        assertThat(barrier.tryCompleteWarmup()).isTrue();
        assertThat(barrier.hasCompletedWarmup()).isTrue();
    }

    @Test
    @DisplayName("once latched, the barrier stays latched even if a task later fails")
    public void latchIsIdempotentAfterCompletion() {
        AtomicReference<WarmupResult> a = new AtomicReference<>(WarmupResult.success());
        WarmupBarrier barrier = new WarmupBarrier(Arrays.asList(
            task("a", FailureScope.SHARED_DEPENDENCY, a)));

        assertThat(barrier.tryCompleteWarmup()).isTrue();
        assertThat(barrier.hasCompletedWarmup()).isTrue();

        // A later failure does not un-latch a one-time startup gate.
        a.set(WarmupResult.failure("blip"));
        assertThat(barrier.hasCompletedWarmup()).isTrue();
    }

    @Test
    @DisplayName("a task that throws is treated as a failure, not propagated")
    public void throwingTaskIsCaught() {
        WarmupTask throwing = new WarmupTask() {
            @Override
            public String name() {
                return "boom";
            }

            @Override
            public int priority() {
                return 0;
            }

            @Override
            public CompletableFuture<WarmupResult> warmup() {
                throw new IllegalStateException("boom");
            }

            @Override
            public FailureScope failureScope() {
                return FailureScope.LOCAL_FATAL;
            }
        };
        WarmupBarrier barrier = new WarmupBarrier(Arrays.asList(throwing));

        assertThat(barrier.tryCompleteWarmup()).isFalse();
        assertThat(barrier.hasCompletedWarmup()).isFalse();
    }
}
