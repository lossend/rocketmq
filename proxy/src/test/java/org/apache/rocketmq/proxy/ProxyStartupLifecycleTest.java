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

package org.apache.rocketmq.proxy;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.lifecycle.LifecycleScheduler;
import org.apache.rocketmq.proxy.lifecycle.ProxyLifecycleCoordinator;
import org.apache.rocketmq.proxy.lifecycle.ProxyLifecycleState;
import org.apache.rocketmq.proxy.lifecycle.SendDrainGate;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyStartupLifecycleTest {

    @Test
    @DisplayName("production assembly publishes READY only after every startup component succeeds")
    public void successfulStartupPublishesReady() throws Exception {
        ProxyLifecycleCoordinator coordinator = newCoordinator();
        AtomicBoolean componentStarted = new AtomicBoolean();
        StartAndShutdown startup = new StartAndShutdown() {
            @Override
            public void start() {
                componentStarted.set(true);
            }

            @Override
            public void shutdown() {
            }
        };

        ProxyStartup.startAndSignalReady(startup, coordinator, coordinator::onStartupComplete);

        assertThat(componentStarted).isTrue();
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.READY);
        assertThat(coordinator.isStarted()).isTrue();
        assertThat(coordinator.isReady()).isTrue();
    }

    @Test
    @DisplayName("a startup component failure leaves the production coordinator in STARTING")
    public void failedStartupNeverPublishesReady() {
        ProxyLifecycleCoordinator coordinator = newCoordinator();
        IllegalStateException failure = new IllegalStateException("component failed");
        StartAndShutdown startup = new StartAndShutdown() {
            @Override
            public void start() {
                throw failure;
            }

            @Override
            public void shutdown() {
            }
        };

        assertThatThrownBy(() -> ProxyStartup.startAndSignalReady(startup, coordinator,
            coordinator::onStartupComplete))
            .isSameAs(failure);
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.STARTING);
        assertThat(coordinator.isStarted()).isFalse();
        assertThat(coordinator.isReady()).isFalse();
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
