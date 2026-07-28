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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProxyLifecycleCoordinatorTest {

    private final AtomicLong clock = new AtomicLong(1_000_000_000L);
    private final ImmediateScheduler scheduler = new ImmediateScheduler();

    private ProxyLifecycleCoordinator newCoordinator(FakeAdapter adapter, Runnable withdraw) {
        return new ProxyLifecycleCoordinator(new SendDrainGate(), Collections.singletonList(adapter),
            scheduler, clock::get, true, withdraw,
            60, 300, 0.10, 30, 30, 480);
    }

    @Test
    @DisplayName("startup completion atomically publishes started and ready")
    public void startupCompletePublishesReady() {
        ProxyLifecycleCoordinator coordinator = newCoordinator(new FakeAdapter(), null);

        assertThat(coordinator.isStarted()).isFalse();
        assertThat(coordinator.isReady()).isFalse();

        coordinator.onStartupComplete();

        assertThat(coordinator.isStarted()).isTrue();
        assertThat(coordinator.isReady()).isTrue();
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.READY);
    }

    @Test
    @DisplayName("repeated beginDrain returns the identical DrainRun instance")
    public void reentrantDrainReusesRun() {
        FakeAdapter adapter = new FakeAdapter();
        ProxyLifecycleCoordinator coordinator = newCoordinator(adapter, null);
        coordinator.onStartupComplete();
        DrainRun first = coordinator.beginDrain(DrainTrigger.PRESTOP);
        DrainRun second = coordinator.beginDrain(DrainTrigger.ADMIN);
        assertThat(second).isSameAs(first);
        assertThat(second.session()).isSameAs(first.session());
        assertThat(second.drainFuture()).isSameAs(first.drainFuture());
    }

    @Test
    @DisplayName("a normal drain follows migrate -> no-new-work -> close admission -> DRAINED")
    public void normalDrainReachesDrained() {
        FakeAdapter adapter = new FakeAdapter();
        AtomicInteger withdrawn = new AtomicInteger();
        ProxyLifecycleCoordinator coordinator = newCoordinator(adapter, withdrawn::incrementAndGet);
        coordinator.onStartupComplete();

        DrainRun run = coordinator.beginDrain(DrainTrigger.PRESTOP);
        assertThat(withdrawn.get()).isEqualTo(1);
        // Migration only begins after the lb cutoff elapses (see migrationWaitsForLbCutoff).
        scheduler.runScheduledAt(0);
        // Migration was started before admission closed.
        assertThat(adapter.migrationStarted).isTrue();

        adapter.noNewWork.complete(null);
        adapter.terminated.complete(null);

        assertThat(run.drainFuture()).isCompleted();
        assertThat(run.drainFuture().getNow(null).isForced()).isFalse();
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.DRAINED);
    }

    @Test
    @DisplayName("admission stays open until no-new-work is reached, then closes exactly once")
    public void admissionClosesOnlyAfterNoNewWork() {
        FakeAdapter adapter = new FakeAdapter();
        SendDrainGate gate = new SendDrainGate();
        ProxyLifecycleCoordinator coordinator = new ProxyLifecycleCoordinator(gate,
            Collections.singletonList(adapter), scheduler, clock::get, true, null,
            60, 300, 0.10, 30, 30, 480);
        coordinator.onStartupComplete();

        coordinator.beginDrain(DrainTrigger.PRESTOP);
        scheduler.runScheduledAt(0);   // elapse the lb cutoff so migration starts
        // Migration issued but no-new-work not yet reached: gate must stay open.
        assertThat(adapter.migrationStarted).isTrue();
        assertThat(gate.isAdmissionClosed()).isFalse();

        adapter.noNewWork.complete(null);
        assertThat(gate.isAdmissionClosed()).isTrue();
    }

    @Test
    @DisplayName("migration waits for the lb cutoff so the provider can deregister the target first")
    public void migrationWaitsForLbCutoff() {
        FakeAdapter adapter = new FakeAdapter();
        AtomicInteger withdrawn = new AtomicInteger();
        ProxyLifecycleCoordinator coordinator = newCoordinator(adapter, withdrawn::incrementAndGet);
        coordinator.onStartupComplete();

        DrainRun run = coordinator.beginDrain(DrainTrigger.PRESTOP);

        // Readiness is withdrawn immediately, but migration must NOT start yet: the
        // provider still needs time to stop routing new connections to this Pod.
        assertThat(withdrawn.get()).isEqualTo(1);
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.QUIESCING);
        assertThat(adapter.migrationStarted).isFalse();

        // Fire the lb-cutoff timer: only now may migration begin.
        scheduler.runScheduledAt(0);
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.MIGRATING);
        assertThat(adapter.migrationStarted).isTrue();

        adapter.noNewWork.complete(null);
        adapter.terminated.complete(null);
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.DRAINED);
        assertThat(run.drainFuture().getNow(null).isForced()).isFalse();
    }

    @Test
    @DisplayName("an already-quiet listener still does not shorten the lb cutoff wait")
    public void quietObservationNeverShortensWait() {
        FakeAdapter adapter = new FakeAdapter();
        ProxyLifecycleCoordinator coordinator = newCoordinator(adapter, null);
        ProxyLifecycleMetrics metrics = new ProxyLifecycleMetrics();
        // Report a long-standing quiet window (well past the 20s threshold).
        coordinator.observeLbDetachQuiet(() -> TimeUnit.SECONDS.toNanos(120), 20, metrics);
        coordinator.onStartupComplete();

        coordinator.beginDrain(DrainTrigger.PRESTOP);
        // Even though the listener is quiet, migration must still wait for the cutoff.
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.QUIESCING);
        assertThat(adapter.migrationStarted).isFalse();

        scheduler.runScheduledAt(0);
        assertThat(adapter.migrationStarted).isTrue();
        // Quiet threshold was met, so nothing is flagged.
        assertThat(metrics.lbDetachQuietMissed()).isZero();
    }

    @Test
    @DisplayName("a short quiet window flags that the lb detach timeout may be too small")
    public void shortQuietWindowIsFlagged() {
        FakeAdapter adapter = new FakeAdapter();
        ProxyLifecycleCoordinator coordinator = newCoordinator(adapter, null);
        ProxyLifecycleMetrics metrics = new ProxyLifecycleMetrics();
        // New connections were arriving 2s before the cutoff, below the 20s threshold.
        coordinator.observeLbDetachQuiet(() -> TimeUnit.SECONDS.toNanos(2), 20, metrics);
        coordinator.onStartupComplete();

        coordinator.beginDrain(DrainTrigger.PRESTOP);
        scheduler.runScheduledAt(0);

        assertThat(adapter.migrationStarted).isTrue();
        assertThat(metrics.lbDetachQuietMissed()).isEqualTo(1);
    }

    @Test
    @DisplayName("a drain with no quiet observation installed proceeds normally")
    public void noQuietObservationIsHarmless() {
        FakeAdapter adapter = new FakeAdapter();
        ProxyLifecycleCoordinator coordinator = newCoordinator(adapter, null);
        coordinator.onStartupComplete();
        coordinator.beginDrain(DrainTrigger.PRESTOP);
        scheduler.runScheduledAt(0);
        assertThat(adapter.migrationStarted).isTrue();
    }

    @Test
    @DisplayName("a drain from STARTING force-closes admission before returning")
    public void drainFromStartingForces() {
        FakeAdapter adapter = new FakeAdapter();
        SendDrainGate gate = new SendDrainGate();
        ProxyLifecycleCoordinator coordinator = new ProxyLifecycleCoordinator(gate,
            Collections.singletonList(adapter), scheduler, clock::get, true, null,
            60, 300, 0.10, 30, 30, 480);
        DrainRun run = coordinator.beginDrain(DrainTrigger.SIGTERM_FALLBACK);
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.FORCE_DRAINING);
        assertThat(gate.isAdmissionClosed()).isTrue();
        assertThat(gate.tryAcquire(SendProtocol.GRPC)).isEmpty();
        assertThat(run.drainFuture()).isCompleted();
        assertThat(run.drainFuture().getNow(null).isForced()).isTrue();
    }

    @Test
    @DisplayName("a hard deadline in QUIESCING force-closes admission")
    public void forceFromQuiescingClosesAdmission() {
        FakeAdapter adapter = new FakeAdapter();
        SendDrainGate gate = new SendDrainGate();
        ProxyLifecycleCoordinator coordinator = new ProxyLifecycleCoordinator(gate,
            Collections.singletonList(adapter), scheduler, clock::get, true, null,
            60, 300, 0.10, 30, 30, 480);
        coordinator.onStartupComplete();
        coordinator.beginDrain(DrainTrigger.PRESTOP);

        scheduler.runScheduledAt(1);

        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.FORCE_DRAINING);
        assertThat(gate.isAdmissionClosed()).isTrue();
        assertThat(gate.tryAcquire(SendProtocol.GRPC)).isEmpty();
    }

    @Test
    @DisplayName("a hard deadline in MIGRATING force-closes admission")
    public void forceFromMigratingClosesAdmission() {
        FakeAdapter adapter = new FakeAdapter();
        SendDrainGate gate = new SendDrainGate();
        ProxyLifecycleCoordinator coordinator = new ProxyLifecycleCoordinator(gate,
            Collections.singletonList(adapter), scheduler, clock::get, true, null,
            60, 300, 0.10, 30, 30, 480);
        coordinator.onStartupComplete();
        coordinator.beginDrain(DrainTrigger.PRESTOP);
        scheduler.runScheduledAt(0);

        scheduler.runScheduledAt(1);

        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.FORCE_DRAINING);
        assertThat(gate.isAdmissionClosed()).isTrue();
        assertThat(gate.tryAcquire(SendProtocol.GRPC)).isEmpty();
    }

    @Test
    @DisplayName("force transition retries when normal drain advances between state read and CAS")
    public void forceTransitionRetriesAfterConcurrentStateAdvance() {
        AtomicReference<ProxyLifecycleState> scriptedState =
            new AtomicReference<>(ProxyLifecycleState.MIGRATING);
        AtomicInteger casAttempts = new AtomicInteger();

        boolean entered = ProxyLifecycleCoordinator.tryEnterForceDraining(
            scriptedState::get,
            (expected, next) -> {
                if (casAttempts.getAndIncrement() == 0) {
                    assertThat(expected).isEqualTo(ProxyLifecycleState.MIGRATING);
                    assertThat(scriptedState.compareAndSet(
                        ProxyLifecycleState.MIGRATING, ProxyLifecycleState.DRAINING)).isTrue();
                    return false;
                }
                return scriptedState.compareAndSet(expected, next);
            });

        assertThat(entered).isTrue();
        assertThat(casAttempts.get()).isEqualTo(2);
        assertThat(scriptedState.get()).isEqualTo(ProxyLifecycleState.FORCE_DRAINING);
    }

    @Test
    @DisplayName("adapter force failures are retained in the forced drain result")
    public void forceFailuresAreReported() {
        RuntimeException forceFailure = new RuntimeException("force failed");
        FakeAdapter adapter = new FakeAdapter();
        adapter.forceFailure = forceFailure;
        ProxyLifecycleCoordinator coordinator = newCoordinator(adapter, null);

        DrainResult result = coordinator.beginDrain(DrainTrigger.SIGTERM_FALLBACK)
            .drainFuture().getNow(null);

        assertThat(result.isForced()).isTrue();
        assertThat(result.causes()).containsExactly(forceFailure);
    }

    @Test
    @DisplayName("forced remains sticky in snapshots after the state advances to STOPPED")
    public void forcedOutcomeRemainsStickyAfterStop() {
        ProxyLifecycleCoordinator coordinator = newCoordinator(new FakeAdapter(), null);
        coordinator.beginDrain(DrainTrigger.SIGTERM_FALLBACK);

        assertThat(coordinator.snapshot().forced()).isTrue();
        assertThat(coordinator.transition(ProxyLifecycleState.FORCE_DRAINING,
            ProxyLifecycleState.STOPPING, "stopping")).isTrue();
        assertThat(coordinator.transition(ProxyLifecycleState.STOPPING,
            ProxyLifecycleState.STOPPED, "stopped")).isTrue();

        assertThat(coordinator.snapshot().forced()).isTrue();
    }

    @Test
    @DisplayName("hard-deadline timer forces the drain when adapters never complete")
    public void hardDeadlineForces() {
        FakeAdapter adapter = new FakeAdapter();
        ProxyLifecycleCoordinator coordinator = newCoordinator(adapter, null);
        coordinator.onStartupComplete();
        DrainRun run = coordinator.beginDrain(DrainTrigger.PRESTOP);
        assertThat(run.drainFuture()).isNotCompleted();
        // Fire the scheduled hard-deadline task.
        scheduler.runScheduled();
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.FORCE_DRAINING);
        assertThat(run.drainFuture().getNow(null).isForced()).isTrue();
        assertThat(adapter.forced).isTrue();
    }

    @Test
    @DisplayName("escalateForStop cancels timers, closes the gate, and forces adapters with the stop deadline")
    public void escalateForStopFreezesImmediately() {
        FakeAdapter adapter = new FakeAdapter();
        SendDrainGate gate = new SendDrainGate();
        ProxyLifecycleCoordinator coordinator = new ProxyLifecycleCoordinator(gate,
            Collections.singletonList(adapter), scheduler, clock::get, true, null,
            60, 300, 0.10, 30, 30, 480);
        coordinator.onStartupComplete();
        coordinator.beginDrain(DrainTrigger.PRESTOP);

        ShutdownDeadline stop = new ShutdownDeadline(clock.get() + 30_000_000_000L, clock::get);
        CompletableFuture<Void> issued = coordinator.escalateForStop(stop);

        assertThat(issued).isCompleted();
        assertThat(gate.isAdmissionClosed()).isTrue();
        assertThat(adapter.forced).isTrue();
        assertThat(adapter.forcedDeadline).isSameAs(stop);
        assertThat(scheduler.cancelledCount).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("the accepted-send release thread never invokes transport termination")
    public void permitReleaseDoesNotInvokeAwaitTerminated() throws Exception {
        AsyncScheduler asyncScheduler = new AsyncScheduler();
        try {
            FakeAdapter adapter = new FakeAdapter();
            adapter.noNewWork.complete(null);
            adapter.terminated.complete(null);
            SendDrainGate gate = new SendDrainGate();
            SendPermit permit = gate.tryAcquire(SendProtocol.GRPC).get();
            permit.backendStarted();
            ProxyLifecycleCoordinator coordinator = new ProxyLifecycleCoordinator(gate,
                Collections.singletonList(adapter), asyncScheduler, System::nanoTime, true, null,
                0, 0, 0.0, 0, 30, 0);
            coordinator.onStartupComplete();

            coordinator.beginDrain(DrainTrigger.PRESTOP);
            asyncScheduler.awaitIdle();
            asyncScheduler.runScheduledAt(0);
            assertThat(adapter.awaitThread).isNull();

            Thread tracerThread = new Thread(() -> {
                permit.backendTerminal(null);
                permit.protocolTerminal(ProtocolResult.success());
            }, "test-grpc-tracer");
            tracerThread.start();
            tracerThread.join(TimeUnit.SECONDS.toMillis(5));
            asyncScheduler.awaitIdle();

            assertThat(tracerThread.isAlive()).isFalse();
            assertThat(adapter.awaitThread).isNotNull();
            assertThat(adapter.awaitThread.getName()).isEqualTo("test-lifecycle-scheduler");
            assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.DRAINED);
        } finally {
            asyncScheduler.close();
        }
    }

    // --- fakes -------------------------------------------------------------

    private static final class FakeAdapter implements DrainProtocolAdapter {
        volatile boolean migrationStarted;
        volatile boolean forced;
        volatile ShutdownDeadline forcedDeadline;
        volatile RuntimeException forceFailure;
        volatile Thread awaitThread;
        final CompletableFuture<Void> noNewWork = new CompletableFuture<>();
        final CompletableFuture<Void> terminated = new CompletableFuture<>();

        @Override
        public SendProtocol protocol() {
            return SendProtocol.GRPC;
        }

        @Override
        public void startMigration() {
            migrationStarted = true;
        }

        @Override
        public CompletableFuture<Void> noNewWorkReached() {
            return noNewWork;
        }

        @Override
        public CompletableFuture<Void> awaitTerminated(ShutdownDeadline effectiveDeadline) {
            awaitThread = Thread.currentThread();
            return terminated;
        }

        @Override
        public void force(ShutdownDeadline effectiveDeadline) {
            forced = true;
            forcedDeadline = effectiveDeadline;
            if (forceFailure != null) {
                throw forceFailure;
            }
        }
    }

    private static final class AsyncScheduler implements LifecycleScheduler, AutoCloseable {
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

    private static final class ImmediateScheduler implements LifecycleScheduler {
        final List<Runnable> scheduled = new ArrayList<>();
        int cancelledCount;

        /** Fires a single scheduled task by registration order (0 = lb cutoff, 1 = hard deadline). */
        void runScheduledAt(int index) {
            Runnable task = scheduled.get(index);
            if (task != null) {
                task.run();
            }
        }

        @Override
        public void execute(Runnable task) {
            task.run();
        }

        @Override
        public ScheduledHandle schedule(Runnable task, long delayNanos) {
            scheduled.add(task);
            final int index = scheduled.size() - 1;
            return () -> {
                cancelledCount++;
                scheduled.set(index, null);
                return true;
            };
        }

        void runScheduled() {
            for (Runnable r : new ArrayList<>(scheduled)) {
                if (r != null) {
                    r.run();
                }
            }
        }
    }
}
