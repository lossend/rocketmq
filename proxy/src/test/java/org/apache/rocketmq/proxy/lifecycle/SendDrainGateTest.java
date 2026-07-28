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

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SendDrainGateTest {

    private void completePermit(SendPermit permit) {
        permit.backendStarted();
        permit.backendTerminal(null);
        permit.protocolTerminal(ProtocolResult.success());
    }

    @Test
    @DisplayName("acquire before close succeeds and counts the accepted send")
    public void acquireBeforeClose() {
        SendDrainGate gate = new SendDrainGate();
        Optional<SendPermit> permit = gate.tryAcquire(SendProtocol.GRPC);
        assertThat(permit).isPresent();
        assertThat(gate.acceptedCount()).isEqualTo(1);
        assertThat(gate.isAdmissionClosed()).isFalse();
    }

    @Test
    @DisplayName("acquire after close is rejected")
    public void acquireAfterCloseRejected() {
        SendDrainGate gate = new SendDrainGate();
        gate.closeAdmission();
        assertThat(gate.tryAcquire(SendProtocol.GRPC)).isEmpty();
    }

    @Test
    @DisplayName("closing with zero accepted completes the drained future immediately")
    public void closeWithZeroCompletesDrained() {
        SendDrainGate gate = new SendDrainGate();
        assertThat(gate.closeAdmission()).isZero();
        assertThat(gate.drainedFuture()).isCompleted();
    }

    @Test
    @DisplayName("drained future completes only after all accepted permits release")
    public void drainedAfterPermitsRelease() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit p1 = gate.tryAcquire(SendProtocol.GRPC).get();
        SendPermit p2 = gate.tryAcquire(SendProtocol.GRPC).get();
        assertThat(gate.closeAdmission()).isEqualTo(2);
        assertThat(gate.drainedFuture()).isNotCompleted();
        completePermit(p1);
        assertThat(gate.drainedFuture()).isNotCompleted();
        completePermit(p2);
        assertThat(gate.drainedFuture()).isCompleted();
    }

    @Test
    @DisplayName("permit releases only when both backend and protocol are terminal, in either order")
    public void dualTerminalBothOrders() {
        SendDrainGate gate = new SendDrainGate();
        // backend first
        SendPermit a = gate.tryAcquire(SendProtocol.GRPC).get();
        a.backendStarted();
        a.backendTerminal(null);
        assertThat(a.releasedFuture()).isNotCompleted();
        a.protocolTerminal(ProtocolResult.success());
        assertThat(a.releasedFuture()).isCompleted();
        // protocol first
        SendPermit b = gate.tryAcquire(SendProtocol.GRPC).get();
        b.protocolTerminal(ProtocolResult.success());
        assertThat(b.releasedFuture()).isNotCompleted();
        b.backendStarted();
        b.backendTerminal(null);
        assertThat(b.releasedFuture()).isCompleted();
    }

    @Test
    @DisplayName("skipped backend plus protocol terminal also releases the permit")
    public void skippedBackendReleases() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit p = gate.tryAcquire(SendProtocol.GRPC).get();
        assertThat(p.tryBackendSkipped(SkipReason.SYNC_VALIDATION)).isTrue();
        p.protocolTerminal(ProtocolResult.success());
        assertThat(p.releasedFuture()).isCompleted();
        assertThat(gate.acceptedCount()).isZero();
    }

    @Test
    @DisplayName("repeated terminal callbacks are idempotent and never underflow the gate")
    public void repeatedCallbacksIdempotent() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit p = gate.tryAcquire(SendProtocol.GRPC).get();
        p.backendStarted();
        p.backendTerminal(null);
        p.backendTerminal(new RuntimeException("dup"));
        p.protocolTerminal(ProtocolResult.success());
        p.protocolTerminal(ProtocolResult.failure(new RuntimeException("dup")));
        assertThat(gate.acceptedCount()).isZero();
        assertThat(p.releasedFuture()).isCompleted();
    }

    @Test
    @DisplayName("tryBackendSkipped loses to an already-started backend without mutating state")
    public void skippedLosesToStarted() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit p = gate.tryAcquire(SendProtocol.GRPC).get();
        assertThat(p.backendStarted()).isTrue();
        assertThat(p.tryBackendSkipped(SkipReason.CANCELLED_BEFORE_DISPATCH)).isFalse();
        // backend is still STARTED: a real terminal is required, permit not yet released
        p.protocolTerminal(ProtocolResult.success());
        assertThat(p.releasedFuture()).isNotCompleted();
        p.backendTerminal(null);
        assertThat(p.releasedFuture()).isCompleted();
    }

    @Test
    @DisplayName("backendTerminal from a non-started state is an invariant violation")
    public void backendTerminalFromNotStartedThrows() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit p = gate.tryAcquire(SendProtocol.GRPC).get();
        try {
            p.backendTerminal(null);
            org.junit.Assert.fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            assertThat(expected).hasMessageContaining("STARTED");
        }
    }

    @Test
    @DisplayName("thousands of concurrent acquires racing close never leak a late permit")
    public void concurrentAcquireCloseRace() throws Exception {
        for (int round = 0; round < 20; round++) {
            final SendDrainGate gate = new SendDrainGate();
            final int threads = 64;
            final ExecutorService pool = Executors.newFixedThreadPool(threads);
            final CountDownLatch start = new CountDownLatch(1);
            final AtomicInteger acquired = new AtomicInteger();
            final java.util.List<SendPermit> permits =
                java.util.Collections.synchronizedList(new java.util.ArrayList<>());
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    await(start);
                    for (int j = 0; j < 50; j++) {
                        Optional<SendPermit> p = gate.tryAcquire(SendProtocol.GRPC);
                        if (p.isPresent()) {
                            acquired.incrementAndGet();
                            permits.add(p.get());
                        }
                    }
                });
            }
            pool.submit(() -> {
                await(start);
                gate.closeAdmission();
            });
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            // No acquire may succeed after close observed; count must match the permits captured.
            assertThat(gate.acceptedCount()).isEqualTo(acquired.get());
            assertThat(gate.isAdmissionClosed()).isTrue();
            // Releasing every captured permit must drain to zero exactly once.
            for (SendPermit p : permits) {
                completePermit(p);
            }
            assertThat(gate.acceptedCount()).isZero();
            assertThat(gate.drainedFuture()).isCompleted();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
