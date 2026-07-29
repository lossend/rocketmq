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

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.apache.rocketmq.proxy.lifecycle.ReadinessResult;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;

public class ReadinessEvaluatorTest {

    private static ReadinessProbe probe(String name, FailureScope scope, AtomicReference<ReadinessResult> holder) {
        return new ReadinessProbe() {
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
    @DisplayName("not ready for traffic before warmup has latched, even if every probe passes")
    public void notReadyBeforeWarmupCompleted() {
        AtomicReference<ReadinessResult> dep = new AtomicReference<>(ReadinessResult.success());
        ReadinessEvaluator evaluator = new ReadinessEvaluator(
            Collections.singletonList(probe("nameserver", FailureScope.SHARED_DEPENDENCY, dep)),
            1, () -> false);

        assertThat(evaluator.isReadyForTraffic()).isFalse();
    }

    @Test
    @DisplayName("a shared-dependency blip fails closed once the threshold is reached")
    public void sharedDependencyFailsCloseAtThreshold() {
        AtomicReference<ReadinessResult> dep = new AtomicReference<>(ReadinessResult.success());
        ReadinessEvaluator evaluator = new ReadinessEvaluator(
            Collections.singletonList(probe("nameserver", FailureScope.SHARED_DEPENDENCY, dep)),
            1, () -> true);

        assertThat(evaluator.isReadyForTraffic()).isTrue();

        dep.set(ReadinessResult.failure("nameserver blip"));
        assertThat(evaluator.isReadyForTraffic()).isFalse();
    }

    @Test
    @DisplayName("the fail-close threshold tolerates fewer shared-dependency failures than its limit")
    public void thresholdTolerantBelowLimit() {
        AtomicReference<ReadinessResult> dep = new AtomicReference<>(ReadinessResult.failure("blip"));
        ReadinessEvaluator evaluator = new ReadinessEvaluator(
            Collections.singletonList(probe("dep", FailureScope.SHARED_DEPENDENCY, dep)),
            2, () -> true);

        // one shared failure with threshold 2 stays serving
        assertThat(evaluator.isReadyForTraffic()).isTrue();
    }

    @Test
    @DisplayName("a local fatal probe fails immediately regardless of threshold")
    public void localFatalFailsImmediately() {
        AtomicReference<ReadinessResult> listener = new AtomicReference<>(ReadinessResult.failure("down"));
        ReadinessEvaluator evaluator = new ReadinessEvaluator(
            Collections.singletonList(probe("grpc-listener", FailureScope.LOCAL_FATAL, listener)),
            3, () -> true);

        assertThat(evaluator.isReadyForTraffic()).isFalse();
    }

    @Test
    @DisplayName("markLocalFatal is sticky and never recovers")
    public void markLocalFatalIsSticky() {
        AtomicReference<ReadinessResult> dep = new AtomicReference<>(ReadinessResult.success());
        ReadinessEvaluator evaluator = new ReadinessEvaluator(
            Collections.singletonList(probe("dep", FailureScope.SHARED_DEPENDENCY, dep)),
            1, () -> true);

        assertThat(evaluator.isReadyForTraffic()).isTrue();
        evaluator.markLocalFatal();
        assertThat(evaluator.isReadyForTraffic()).isFalse();

        // recovery of the probe does not un-fail an explicit local fatal latch
        dep.set(ReadinessResult.success());
        assertThat(evaluator.isReadyForTraffic()).isFalse();
    }

    @Test
    @DisplayName("a probe that throws is treated as a failure, not propagated")
    public void throwingProbeIsCaught() {
        ReadinessProbe throwing = new ReadinessProbe() {
            @Override
            public String name() {
                return "boom";
            }

            @Override
            public CompletableFuture<ReadinessResult> check() {
                throw new IllegalStateException("boom");
            }

            @Override
            public FailureScope failureScope() {
                return FailureScope.LOCAL_FATAL;
            }
        };
        ReadinessEvaluator evaluator = new ReadinessEvaluator(Collections.singletonList(throwing), 1, () -> true);

        assertThat(evaluator.isReadyForTraffic()).isFalse();
    }
}
