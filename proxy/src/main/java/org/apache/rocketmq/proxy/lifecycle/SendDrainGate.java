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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Linearizable admission gate. A single {@link AtomicLong} packs the closed bit
 * (top bit) and the accepted-send count (low 63 bits). The successful acquire
 * CAS is the admission linearization point: once {@link #closeAdmission()}
 * returns, no new permit can be created, and every permit acquired before the
 * close is counted. {@code drainedFuture} completes when admission is closed and
 * the accepted count has drained to zero.
 */
public final class SendDrainGate {

    private static final long CLOSED_BIT = 1L << 63;
    private static final long COUNT_MASK = ~CLOSED_BIT;

    private final AtomicLong state = new AtomicLong(0L);
    private final CompletableFuture<Void> drainedFuture = new CompletableFuture<>();

    private static boolean isClosed(long s) {
        return (s & CLOSED_BIT) != 0;
    }

    private static long count(long s) {
        return s & COUNT_MASK;
    }

    /**
     * Attempts to admit one send. Returns empty if admission is already closed.
     * On success the returned permit is counted and must be completed so its
     * release decrements the gate.
     */
    public Optional<SendPermit> tryAcquire(SendProtocol protocol) {
        while (true) {
            long cur = state.get();
            if (isClosed(cur)) {
                return Optional.empty();
            }
            long curCount = count(cur);
            if (curCount == COUNT_MASK) {
                throw new IllegalStateException("SendDrainGate accepted count overflow");
            }
            long next = (cur & CLOSED_BIT) | (curCount + 1);
            if (state.compareAndSet(cur, next)) {
                return Optional.of(new SendPermit(protocol, this::onPermitReleased));
            }
        }
    }

    private void onPermitReleased() {
        while (true) {
            long cur = state.get();
            long curCount = count(cur);
            if (curCount == 0) {
                // Should never happen; guard against underflow rather than corrupt the closed bit.
                throw new IllegalStateException("SendDrainGate accepted count underflow");
            }
            long next = (cur & CLOSED_BIT) | (curCount - 1);
            if (state.compareAndSet(cur, next)) {
                if (isClosed(next) && count(next) == 0) {
                    drainedFuture.complete(null);
                }
                return;
            }
        }
    }

    /**
     * Closes admission. Returns the accepted count captured atomically with the
     * close. If the count is already zero, completes {@code drainedFuture}.
     */
    public long closeAdmission() {
        while (true) {
            long cur = state.get();
            long next = cur | CLOSED_BIT;
            if (state.compareAndSet(cur, next)) {
                long remaining = count(next);
                if (remaining == 0) {
                    drainedFuture.complete(null);
                }
                return remaining;
            }
        }
    }

    public boolean isAdmissionClosed() {
        return isClosed(state.get());
    }

    public long acceptedCount() {
        return count(state.get());
    }

    public CompletableFuture<Void> drainedFuture() {
        return drainedFuture;
    }
}
