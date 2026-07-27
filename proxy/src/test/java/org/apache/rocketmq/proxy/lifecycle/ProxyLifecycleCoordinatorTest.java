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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
    @DisplayName("repeated beginDrain returns the identical DrainRun instance")
    public void reentrantDrainReusesRun() {
        FakeAdapter adapter = new FakeAdapter();
        ProxyLifecycleCoordinator coordinator = newCoordinator(adapter, null);
        coordinator.markReady();
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
        coordinator.markReady();

        DrainRun run = coordinator.beginDrain(DrainTrigger.PRESTOP);
        // Migration was started before admission closed.
        assertThat(adapter.migrationStarted).isTrue();
        assertThat(withdrawn.get()).isEqualTo(1);

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
        coordinator.markReady();

        coordinator.beginDrain(DrainTrigger.PRESTOP);
        // Migration issued but no-new-work not yet reached: gate must stay open.
        assertThat(adapter.migrationStarted).isTrue();
        assertThat(gate.isAdmissionClosed()).isFalse();

        adapter.noNewWork.complete(null);
        assertThat(gate.isAdmissionClosed()).isTrue();
    }

    @Test
    @DisplayName("a drain from STARTING escalates straight to FORCE_DRAINING")
    public void drainFromStartingForces() {
        FakeAdapter adapter = new FakeAdapter();
        ProxyLifecycleCoordinator coordinator = newCoordinator(adapter, null);
        DrainRun run = coordinator.beginDrain(DrainTrigger.SIGTERM_FALLBACK);
        assertThat(coordinator.state()).isEqualTo(ProxyLifecycleState.FORCE_DRAINING);
        assertThat(run.drainFuture()).isCompleted();
        assertThat(run.drainFuture().getNow(null).isForced()).isTrue();
    }

    @Test
    @DisplayName("hard-deadline timer forces the drain when adapters never complete")
    public void hardDeadlineForces() {
        FakeAdapter adapter = new FakeAdapter();
        ProxyLifecycleCoordinator coordinator = newCoordinator(adapter, null);
        coordinator.markReady();
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
        coordinator.markReady();
        coordinator.beginDrain(DrainTrigger.PRESTOP);

        ShutdownDeadline stop = new ShutdownDeadline(clock.get() + 30_000_000_000L, clock::get);
        CompletableFuture<Void> issued = coordinator.escalateForStop(stop);

        assertThat(issued).isCompleted();
        assertThat(gate.isAdmissionClosed()).isTrue();
        assertThat(adapter.forced).isTrue();
        assertThat(adapter.forcedDeadline).isSameAs(stop);
        assertThat(scheduler.cancelledCount).isGreaterThanOrEqualTo(1);
    }

    // --- fakes -------------------------------------------------------------

    private static final class FakeAdapter implements DrainProtocolAdapter {
        volatile boolean migrationStarted;
        volatile boolean forced;
        volatile ShutdownDeadline forcedDeadline;
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
            return terminated;
        }

        @Override
        public void force(ShutdownDeadline effectiveDeadline) {
            forced = true;
            forcedDeadline = effectiveDeadline;
        }
    }

    private static final class ImmediateScheduler implements LifecycleScheduler {
        final List<Runnable> scheduled = new ArrayList<>();
        int cancelledCount;

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
