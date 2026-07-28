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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.proxy.lifecycle.DrainRun;
import org.apache.rocketmq.proxy.lifecycle.DrainTrigger;
import org.apache.rocketmq.proxy.lifecycle.LifecycleScheduler;
import org.apache.rocketmq.proxy.lifecycle.ProxyLifecycleCoordinator;
import org.apache.rocketmq.proxy.lifecycle.ProxyLifecycleState;
import org.apache.rocketmq.proxy.lifecycle.SendDrainGate;
import org.apache.rocketmq.proxy.lifecycle.ShutdownDeadline;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GrpcDrainAdapterTest {

    private static final class FakePhasedServer implements GrpcDrainAdapter.PhasedGrpcServer {
        final AtomicInteger initiated = new AtomicInteger();
        final AtomicInteger forced = new AtomicInteger();
        volatile boolean terminate = true;
        volatile CountDownLatch awaitEntered;
        volatile CountDownLatch awaitRelease;
        volatile Thread awaitThread;

        @Override
        public void initiateServerDrain() {
            initiated.incrementAndGet();
        }

        @Override
        public boolean awaitServerTermination(ShutdownDeadline deadline) throws InterruptedException {
            awaitThread = Thread.currentThread();
            if (awaitEntered != null) {
                awaitEntered.countDown();
            }
            if (awaitRelease != null) {
                awaitRelease.await();
            }
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
            new GrpcDrainStatusPolicy(), Runnable::run);
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
        GrpcDrainAdapter adapter = new GrpcDrainAdapter(server, registry, new GrpcDrainStatusPolicy(),
            Runnable::run);

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
            new GrpcDrainStatusPolicy(), Runnable::run);

        CompletableFuture<Void> terminated = adapter.awaitTerminated(deadline());
        assertThat(terminated).isCompletedExceptionally();
        assertThat(server.forced.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("force is idempotent and shuts the server down now")
    public void forceIdempotent() {
        FakePhasedServer server = new FakePhasedServer();
        GrpcDrainAdapter adapter = new GrpcDrainAdapter(server, new GrpcActiveCallRegistry(),
            new GrpcDrainStatusPolicy(), Runnable::run);
        adapter.force(deadline());
        adapter.force(deadline());
        assertThat(server.forced.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("awaitTerminated runs the blocking server wait on the managed termination executor")
    public void awaitTerminatedUsesManagedExecutor() throws Exception {
        FakePhasedServer server = new FakePhasedServer();
        server.awaitEntered = new CountDownLatch(1);
        server.awaitRelease = new CountDownLatch(1);
        ExecutorService terminationExecutor = Executors.newSingleThreadExecutor(
            task -> new Thread(task, "test-grpc-terminator"));
        try {
            GrpcDrainAdapter adapter = new GrpcDrainAdapter(server, new GrpcActiveCallRegistry(),
                new GrpcDrainStatusPolicy(), terminationExecutor);
            Thread completionThread = Thread.currentThread();

            CompletableFuture<Void> terminated = adapter.awaitTerminated(deadline());

            assertThat(server.awaitEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(terminated).isNotCompleted();
            assertThat(server.awaitThread).isNotSameAs(completionThread);
            assertThat(server.awaitThread.getName()).isEqualTo("test-grpc-terminator");

            server.awaitRelease.countDown();
            terminated.get(5, TimeUnit.SECONDS);
        } finally {
            server.awaitRelease.countDown();
            terminationExecutor.shutdownNow();
        }
    }

    @Test
    @DisplayName("the hard deadline can force while server termination is blocked")
    public void hardDeadlineRunsWhileTerminationWaits() throws Exception {
        FakePhasedServer server = new FakePhasedServer();
        server.awaitEntered = new CountDownLatch(1);
        server.awaitRelease = new CountDownLatch(1);
        ExecutorService terminationExecutor = Executors.newSingleThreadExecutor(
            task -> new Thread(task, "test-grpc-terminator"));
        ControlledScheduler lifecycleScheduler = new ControlledScheduler();
        try {
            GrpcDrainAdapter adapter = new GrpcDrainAdapter(server, new GrpcActiveCallRegistry(),
                new GrpcDrainStatusPolicy(), terminationExecutor);
            ProxyLifecycleCoordinator coordinator = new ProxyLifecycleCoordinator(
                new SendDrainGate(), Collections.singletonList(adapter), lifecycleScheduler,
                System::nanoTime, true, null, 60, 300, 0.1, 30, 30, 480);
            coordinator.onStartupComplete();
            DrainRun run = coordinator.beginDrain(DrainTrigger.PRESTOP);
            lifecycleScheduler.awaitIdle();

            lifecycleScheduler.runScheduledAt(0);
            assertThat(server.awaitEntered.await(5, TimeUnit.SECONDS)).isTrue();

            lifecycleScheduler.runScheduledAt(1);

            assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.FORCE_DRAINING);
            assertThat(run.drainFuture().getNow(null).isForced()).isTrue();
            assertThat(server.forced.get()).isEqualTo(1);
        } finally {
            server.awaitRelease.countDown();
            lifecycleScheduler.close();
            terminationExecutor.shutdownNow();
        }
    }

    private static final class ControlledScheduler implements LifecycleScheduler, AutoCloseable {
        private final List<Runnable> scheduled = Collections.synchronizedList(new ArrayList<>());
        private final ExecutorService executor = Executors.newSingleThreadExecutor(
            task -> new Thread(task, "test-lifecycle-scheduler"));

        @Override
        public void execute(Runnable task) {
            executor.execute(task);
        }

        @Override
        public ScheduledHandle schedule(Runnable task, long delayNanos) {
            scheduled.add(task);
            int index = scheduled.size() - 1;
            return () -> {
                scheduled.set(index, null);
                return true;
            };
        }

        void runScheduledAt(int index) throws Exception {
            Runnable task = scheduled.get(index);
            if (task != null) {
                executor.submit(task).get(5, TimeUnit.SECONDS);
                awaitIdle();
            }
        }

        void awaitIdle() throws Exception {
            executor.submit(() -> {
            }).get(5, TimeUnit.SECONDS);
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }
}
