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

package org.apache.rocketmq.proxy.lifecycle.admin;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.apache.rocketmq.proxy.lifecycle.LifecycleScheduler;
import org.apache.rocketmq.proxy.lifecycle.ProxyLifecycleCoordinator;
import org.apache.rocketmq.proxy.lifecycle.ReadinessResult;
import org.apache.rocketmq.proxy.lifecycle.SendDrainGate;
import org.apache.rocketmq.proxy.lifecycle.readiness.ReadinessEvaluator;
import org.apache.rocketmq.proxy.lifecycle.readiness.ReadinessProbe;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;

public class ReadyForTrafficDivergenceTest {

    @Test
    @DisplayName("after READY, a shared-dependency failure keeps /ready 200 but drops /ready-for-traffic to 503")
    public void sharedDependencyDivergesTheTwoEndpoints() {
        AtomicReference<ReadinessResult> dep = new AtomicReference<>(ReadinessResult.success());
        ReadinessEvaluator evaluator = new ReadinessEvaluator(
            Collections.singletonList(sharedDependency(dep)), 1, () -> true);

        ProxyLifecycleCoordinator coordinator = newCoordinator();
        coordinator.setReadinessEvaluator(evaluator);
        coordinator.onStartupComplete();
        CoordinatorAdminHandlers handlers = new CoordinatorAdminHandlers(coordinator, Runnable::run);

        // both healthy while the dependency is up
        assertThat(handlers.ready().status()).isEqualTo(200);
        assertThat(handlers.readyForTraffic().status()).isEqualTo(200);

        // dependency blip: /ready stays up (fail-open, unaffected by the evaluator),
        // /ready-for-traffic fails closed
        dep.set(ReadinessResult.failure("nameserver blip"));
        assertThat(handlers.ready().status()).isEqualTo(200);
        assertThat(handlers.readyForTraffic().status()).isEqualTo(503);
    }

    @Test
    @DisplayName("without an evaluator installed, /ready-for-traffic falls back to the /ready predicate")
    public void noEvaluatorFallsBackToReady() {
        ProxyLifecycleCoordinator coordinator = newCoordinator();
        coordinator.onStartupComplete();
        CoordinatorAdminHandlers handlers = new CoordinatorAdminHandlers(coordinator, Runnable::run);

        assertThat(handlers.ready().status()).isEqualTo(200);
        assertThat(handlers.readyForTraffic().status()).isEqualTo(200);
    }

    private static ReadinessProbe sharedDependency(AtomicReference<ReadinessResult> holder) {
        return new ReadinessProbe() {
            @Override
            public String name() {
                return "nameserver";
            }

            @Override
            public CompletableFuture<ReadinessResult> check() {
                return CompletableFuture.completedFuture(holder.get());
            }

            @Override
            public FailureScope failureScope() {
                return FailureScope.SHARED_DEPENDENCY;
            }
        };
    }

    private static ProxyLifecycleCoordinator newCoordinator() {
        LifecycleScheduler scheduler = new LifecycleScheduler() {
            @Override
            public void execute(Runnable task) {
                task.run();
            }

            @Override
            public ScheduledHandle schedule(Runnable task, long delayNanos) {
                return () -> false;
            }
        };
        return new ProxyLifecycleCoordinator(new SendDrainGate(), Collections.emptyList(),
            scheduler, System::nanoTime, true, null,
            0, 0, 0.0, 0, 0, 0);
    }
}
