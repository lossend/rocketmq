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

package org.apache.rocketmq.proxy.lifecycle;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ReadinessBarrierTest {

    private static ReadinessContributor contributor(String name, FailureScope scope,
        AtomicReference<ReadinessResult> holder) {
        return new ReadinessContributor() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public CompletableFuture<ReadinessResult> check() {
                return CompletableFuture.completedFuture(holder.get());
            }

            @Override
            public FailureScope failureScope() {
                return scope;
            }
        };
    }

    @Test
    @DisplayName("warmup does not complete until every contributor succeeds once")
    public void warmupRequiresAll() {
        AtomicReference<ReadinessResult> a = new AtomicReference<>(ReadinessResult.failure("cold"));
        AtomicReference<ReadinessResult> b = new AtomicReference<>(ReadinessResult.success());
        ReadinessBarrier barrier = new ReadinessBarrier(Arrays.asList(
            contributor("a", FailureScope.SHARED_DEPENDENCY, a),
            contributor("b", FailureScope.LOCAL_FATAL, b)), 3);

        assertThat(barrier.tryCompleteWarmup()).isFalse();
        assertThat(barrier.hasCompletedWarmup()).isFalse();
        a.set(ReadinessResult.success());
        assertThat(barrier.tryCompleteWarmup()).isTrue();
        assertThat(barrier.hasCompletedWarmup()).isTrue();
    }

    @Test
    @DisplayName("after READY, a shared-dependency blip is fail-open for /ready but fail-close for /ready-for-traffic")
    public void sharedDependencyFailOpenForReady() {
        AtomicReference<ReadinessResult> dep = new AtomicReference<>(ReadinessResult.success());
        ReadinessBarrier barrier = new ReadinessBarrier(Arrays.asList(
            contributor("nameserver", FailureScope.SHARED_DEPENDENCY, dep)), 1);
        assertThat(barrier.tryCompleteWarmup()).isTrue();

        dep.set(ReadinessResult.failure("nameserver blip"));
        assertThat(barrier.isReady()).isTrue();
        assertThat(barrier.isReadyForTraffic()).isFalse();
    }

    @Test
    @DisplayName("a local fatal fails both predicates and never recovers")
    public void localFatalFailsBoth() {
        AtomicReference<ReadinessResult> listener = new AtomicReference<>(ReadinessResult.success());
        ReadinessBarrier barrier = new ReadinessBarrier(Arrays.asList(
            contributor("grpc-listener", FailureScope.LOCAL_FATAL, listener)), 1);
        assertThat(barrier.tryCompleteWarmup()).isTrue();

        listener.set(ReadinessResult.failure("listener down"));
        assertThat(barrier.isReady()).isFalse();
        assertThat(barrier.isReadyForTraffic()).isFalse();

        // recovery of the contributor does not un-fail an explicit local fatal latch
        listener.set(ReadinessResult.success());
        barrier.markLocalFatal();
        assertThat(barrier.isReady()).isFalse();
    }

    @Test
    @DisplayName("the fail-close threshold gates /ready-for-traffic on shared-dependency failures")
    public void dependencyThresholdGatesTraffic() {
        AtomicReference<ReadinessResult> dep = new AtomicReference<>(ReadinessResult.success());
        ReadinessBarrier barrier = new ReadinessBarrier(Arrays.asList(
            contributor("dep", FailureScope.SHARED_DEPENDENCY, dep)), 2);
        assertThat(barrier.tryCompleteWarmup()).isTrue();
        // one shared failure with threshold 2 stays serving
        dep.set(ReadinessResult.failure("blip"));
        assertThat(barrier.isReadyForTraffic()).isTrue();
    }
}
