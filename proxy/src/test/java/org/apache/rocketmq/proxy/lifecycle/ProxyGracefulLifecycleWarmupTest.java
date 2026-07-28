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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.lifecycle.grpc.GrpcDrainAdapter;
import org.apache.rocketmq.proxy.lifecycle.warmup.WarmupRegistry;
import org.apache.rocketmq.proxy.lifecycle.warmup.WarmupTasks;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;

public class ProxyGracefulLifecycleWarmupTest {

    private ProxyConfig strictConfig() {
        ProxyConfig config = new ProxyConfig();
        config.setProxyMode("cluster");
        config.setEnableProxyAdminServer(true);
        config.setEnableProxyGracefulLifecycle(true);
        config.setProxyWarmupTimeoutSeconds(60);
        return config;
    }

    @Test
    @DisplayName("warmup gates READY until the NameServer probe passes, then publishes once")
    public void warmupGatesUntilProbePasses() {
        ProxyGracefulLifecycleWiring wiring = new ProxyGracefulLifecycleWiring();
        QueueingScheduler scheduler = new QueueingScheduler();
        ProxyLifecycleCoordinator coordinator = wiring.createCoordinator(
            new FakePhasedServer(), strictConfig(), scheduler, Runnable::run);

        AtomicBoolean nameServerUp = new AtomicBoolean(false);
        AtomicInteger readyCount = new AtomicInteger();
        WarmupRegistry registry = new WarmupRegistry();
        registry.register(WarmupTasks.nameServerReachable(
            WarmupTasks.PRIORITY_NAMESERVER, nameServerUp::get));

        wiring.startWarmup(registry, strictConfig(), scheduler, () -> {
            readyCount.incrementAndGet();
            coordinator.onStartupComplete();
        });

        // First tick ran inside startWarmup: probe down, still STARTING, retry queued.
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.STARTING);
        assertThat(scheduler.pending()).isEqualTo(1);

        // A retry with the probe still down keeps us STARTING.
        scheduler.runNext();
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.STARTING);

        // Probe recovers; the next tick completes warmup and publishes READY exactly once.
        nameServerUp.set(true);
        scheduler.runNext();
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.READY);
        assertThat(readyCount.get()).isEqualTo(1);
        assertThat(scheduler.pending()).isEqualTo(0);
    }

    @Test
    @DisplayName("registry-driven warmup gates READY until the registered NameServer task passes")
    public void registryDrivenWarmupGatesReady() {
        ProxyGracefulLifecycleWiring wiring = new ProxyGracefulLifecycleWiring();
        QueueingScheduler scheduler = new QueueingScheduler();
        ProxyLifecycleCoordinator coordinator = wiring.createCoordinator(
            new FakePhasedServer(), strictConfig(), scheduler, Runnable::run);

        AtomicBoolean nameServerUp = new AtomicBoolean(false);
        WarmupRegistry registry = new WarmupRegistry();
        registry.register(WarmupTasks.supplied("grpc-listener-bound",
            WarmupTasks.PRIORITY_GRPC_LISTENER, FailureScope.LOCAL_FATAL, () -> true));
        registry.register(WarmupTasks.nameServerReachable(
            WarmupTasks.PRIORITY_NAMESERVER, nameServerUp::get));

        wiring.startWarmup(registry, strictConfig(), scheduler, coordinator::onStartupComplete);

        // NameServer task not yet reachable: still STARTING.
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.STARTING);

        // Task recovers; next tick completes warmup and publishes READY.
        nameServerUp.set(true);
        scheduler.runNext();
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.READY);
    }

    @Test
    @DisplayName("warmup with a broker topic gates READY until at least one broker for it is reachable")
    public void warmupGatesOnBrokerReachability() {
        ProxyGracefulLifecycleWiring wiring = new ProxyGracefulLifecycleWiring();
        QueueingScheduler scheduler = new QueueingScheduler();
        ProxyLifecycleCoordinator coordinator = wiring.createCoordinator(
            new FakePhasedServer(), strictConfig(), scheduler, Runnable::run);

        AtomicBoolean brokerUp = new AtomicBoolean(false);
        WarmupRegistry registry = new WarmupRegistry();
        registry.register(WarmupTasks.nameServerReachable(WarmupTasks.PRIORITY_NAMESERVER, () -> true));
        registry.register(WarmupTasks.brokerReachable("TopicX", WarmupTasks.PRIORITY_BROKER,
            topic -> java.util.Arrays.asList("broker-a:10911", "broker-b:10911"),
            addr -> brokerUp.get()));

        wiring.startWarmup(registry, strictConfig(), scheduler, coordinator::onStartupComplete);

        // NameServer is up but no broker is reachable yet: still STARTING.
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.STARTING);

        // A broker becomes reachable; the next tick completes warmup and publishes READY.
        brokerUp.set(true);
        scheduler.runNext();
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.READY);
    }

    @Test
    @DisplayName("warmup that never completes leaves the coordinator STARTING and keeps retrying")
    public void warmupTimeoutStaysStarting() {
        ProxyGracefulLifecycleWiring wiring = new ProxyGracefulLifecycleWiring();
        QueueingScheduler scheduler = new QueueingScheduler();
        ProxyLifecycleCoordinator coordinator = wiring.createCoordinator(
            new FakePhasedServer(), strictConfig(), scheduler, Runnable::run);

        WarmupRegistry registry = new WarmupRegistry();
        registry.register(WarmupTasks.nameServerReachable(WarmupTasks.PRIORITY_NAMESERVER, () -> false));

        wiring.startWarmup(registry, strictConfig(), scheduler, coordinator::onStartupComplete);

        // Drive many ticks well past any deadline; state must never advance and the
        // loop must keep rescheduling (never markFatal, never STOPPED).
        for (int i = 0; i < 100; i++) {
            assertThat(scheduler.pending()).isEqualTo(1);
            scheduler.runNext();
            assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.STARTING);
        }
        assertThat(coordinator.isLive()).isTrue();
    }

    private static final class FakePhasedServer implements GrpcDrainAdapter.PhasedGrpcServer {
        @Override
        public void initiateServerDrain() {
        }

        @Override
        public boolean awaitServerTermination(ShutdownDeadline deadline) {
            return true;
        }

        @Override
        public void forceServerShutdown() {
        }
    }

    /**
     * Enqueues scheduled tasks instead of running them inline, so a self-rescheduling
     * warmup loop can be stepped one tick at a time without infinite recursion.
     */
    private static final class QueueingScheduler implements LifecycleScheduler {
        private final Deque<Runnable> queue = new ArrayDeque<>();

        @Override
        public void execute(Runnable task) {
            task.run();
        }

        @Override
        public ScheduledHandle schedule(Runnable task, long delayNanos) {
            queue.addLast(task);
            return () -> queue.remove(task);
        }

        int pending() {
            return queue.size();
        }

        void runNext() {
            Runnable task = queue.pollFirst();
            if (task != null) {
                task.run();
            }
        }
    }
}
