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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.lifecycle.admin.CoordinatorAdminHandlers;
import org.apache.rocketmq.proxy.lifecycle.admin.ProxyAdminResponse;
import org.apache.rocketmq.proxy.lifecycle.grpc.GrpcDrainAdapter;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProxyGracefulLifecycleWiringTest {

    private ProxyConfig strictConfig() {
        ProxyConfig config = new ProxyConfig();
        config.setProxyMode("cluster");
        config.setEnableProxyAdminServer(true);
        config.setEnableProxyGracefulLifecycle(true);
        return config;
    }

    private static final class FakePhasedServer implements GrpcDrainAdapter.PhasedGrpcServer {
        final AtomicInteger initiated = new AtomicInteger();

        @Override
        public void initiateServerDrain() {
            initiated.incrementAndGet();
        }

        @Override
        public boolean awaitServerTermination(ShutdownDeadline deadline) {
            return true;
        }

        @Override
        public void forceServerShutdown() {
        }
    }

    @Test
    @DisplayName("the wiring builds gRPC lifecycle components eagerly for the builder")
    public void buildsGrpcComponentsEagerly() {
        ProxyGracefulLifecycleWiring wiring = new ProxyGracefulLifecycleWiring();
        assertThat(wiring.tracerFactory()).isNotNull();
        assertThat(wiring.sendInterceptor()).isNotNull();
        assertThat(wiring.activeCallInterceptor()).isNotNull();
        assertThat(wiring.transportFilter()).isNotNull();
        assertThat(wiring.metrics()).isNotNull();
        // coordinator does not exist until the phased server is available
        assertThat(wiring.coordinator()).isNull();
    }

    @Test
    @DisplayName("createCoordinator publishes a READY-capable coordinator bound to the gRPC adapter")
    public void createCoordinatorPublishes() {
        ProxyGracefulLifecycleWiring wiring = new ProxyGracefulLifecycleWiring();
        FakePhasedServer server = new FakePhasedServer();
        ProxyLifecycleCoordinator coordinator = wiring.createCoordinator(
            server, strictConfig(), new ImmediateScheduler(), Runnable::run);
        assertThat(coordinator).isNotNull();
        assertThat(wiring.coordinator()).isSameAs(coordinator);
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.STARTING);
    }

    @Test
    @DisplayName("an admin drain request drives the coordinator to DRAINED through the gRPC adapter")
    public void adminDrainReachesDrained() {
        ProxyGracefulLifecycleWiring wiring = new ProxyGracefulLifecycleWiring();
        FakePhasedServer server = new FakePhasedServer();
        ProxyLifecycleCoordinator coordinator = wiring.createCoordinator(
            server, strictConfig(), new ImmediateScheduler(), Runnable::run);
        coordinator.onStartupComplete();

        CoordinatorAdminHandlers handlers = new CoordinatorAdminHandlers(coordinator, Runnable::run);

        // /ready is 200 before the drain
        assertThat(handlers.ready().status()).isEqualTo(200);

        // POST /drain returns 202 immediately with a runId
        ProxyAdminResponse drainResponse = handlers.startDrain();
        assertThat(drainResponse.status()).isEqualTo(202);
        assertThat(drainResponse.body()).contains("runId");

        // migration was initiated on the gRPC server, and (fake completes instantly) we reach DRAINED
        assertThat(server.initiated.get()).isEqualTo(1);
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.DRAINED);

        // /ready now reports NOT_READY (503)
        assertThat(handlers.ready().status()).isEqualTo(503);

        // drain status echoes the same runId
        String runId = coordinator.snapshot().drainId();
        assertThat(handlers.drainStatus(runId).status()).isEqualTo(200);
        assertThat(handlers.drainStatus("bogus").status()).isEqualTo(404);
    }

    @Test
    @DisplayName("/state reflects lifecycle-enabled and the current phase")
    public void stateReflectsLifecycle() {
        ProxyGracefulLifecycleWiring wiring = new ProxyGracefulLifecycleWiring();
        ProxyLifecycleCoordinator coordinator = wiring.createCoordinator(
            new FakePhasedServer(), strictConfig(), new ImmediateScheduler(), Runnable::run);
        coordinator.onStartupComplete();
        CoordinatorAdminHandlers handlers = new CoordinatorAdminHandlers(coordinator, Runnable::run);
        ProxyAdminResponse state = handlers.state();
        assertThat(state.status()).isEqualTo(200);
        assertThat(state.body()).contains("\"lifecycleEnabled\":true");
        assertThat(state.body()).contains("\"state\":\"READY\"");
    }

    /**
     * Runs every task inline, including scheduled ones, so a drain walks the whole
     * chain (including the lb-cutoff gate) within the test thread.
     */
    private static final class ImmediateScheduler implements LifecycleScheduler {
        @Override
        public void execute(Runnable task) {
            task.run();
        }

        @Override
        public ScheduledHandle schedule(Runnable task, long delayNanos) {
            task.run();
            return () -> false;
        }
    }
}
