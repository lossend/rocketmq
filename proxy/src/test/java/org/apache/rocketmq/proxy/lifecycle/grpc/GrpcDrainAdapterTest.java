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

package org.apache.rocketmq.proxy.lifecycle.grpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.proxy.lifecycle.ShutdownDeadline;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GrpcDrainAdapterTest {

    private static final class FakePhasedServer implements GrpcDrainAdapter.PhasedGrpcServer {
        final AtomicInteger initiated = new AtomicInteger();
        final AtomicInteger forced = new AtomicInteger();
        volatile boolean terminate = true;

        @Override
        public void initiateServerDrain() {
            initiated.incrementAndGet();
        }

        @Override
        public boolean awaitServerTermination(ShutdownDeadline deadline) {
            return terminate;
        }

        @Override
        public void forceServerShutdown() {
            forced.incrementAndGet();
        }
    }

    private ShutdownDeadline deadline() {
        return ShutdownDeadline.afterNanos(System.nanoTime(),
            java.util.concurrent.TimeUnit.SECONDS.toNanos(30), System::nanoTime);
    }

    @Test
    @DisplayName("startMigration initiates the server drain once and completes no-new-work")
    public void startMigrationInitiatesOnce() {
        FakePhasedServer server = new FakePhasedServer();
        GrpcDrainAdapter adapter = new GrpcDrainAdapter(server, new GrpcActiveCallRegistry(),
            new GrpcDrainStatusPolicy());
        adapter.startMigration();
        adapter.startMigration();
        assertThat(server.initiated.get()).isEqualTo(1);
        assertThat(adapter.noNewWorkReached()).isCompleted();
    }

    @Test
    @DisplayName("awaitTerminated closes the registry then completes once the server terminates")
    public void awaitTerminatedCompletes() {
        FakePhasedServer server = new FakePhasedServer();
        GrpcActiveCallRegistry registry = new GrpcActiveCallRegistry();
        GrpcDrainAdapter adapter = new GrpcDrainAdapter(server, registry, new GrpcDrainStatusPolicy());

        CompletableFuture<Void> terminated = adapter.awaitTerminated(deadline());
        assertThat(registry.isRegistrationClosed()).isTrue();
        assertThat(terminated).isCompleted();
        assertThat(server.forced.get()).isZero();
    }

    @Test
    @DisplayName("awaitTerminated forces and fails when the server does not terminate in time")
    public void awaitTerminatedForcesOnTimeout() {
        FakePhasedServer server = new FakePhasedServer();
        server.terminate = false;
        GrpcDrainAdapter adapter = new GrpcDrainAdapter(server, new GrpcActiveCallRegistry(),
            new GrpcDrainStatusPolicy());

        CompletableFuture<Void> terminated = adapter.awaitTerminated(deadline());
        assertThat(terminated).isCompletedExceptionally();
        assertThat(server.forced.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("force is idempotent and shuts the server down now")
    public void forceIdempotent() {
        FakePhasedServer server = new FakePhasedServer();
        GrpcDrainAdapter adapter = new GrpcDrainAdapter(server, new GrpcActiveCallRegistry(),
            new GrpcDrainStatusPolicy());
        adapter.force(deadline());
        adapter.force(deadline());
        assertThat(server.forced.get()).isEqualTo(1);
    }
}
