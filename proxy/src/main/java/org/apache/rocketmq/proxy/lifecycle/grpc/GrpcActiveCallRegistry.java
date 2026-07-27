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

import io.grpc.Status;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks in-flight non-unary RPCs so drain can proactively end them. A single
 * packed {@link AtomicLong} (top bit = registration closed, low 63 = open count)
 * linearizes register/closeAll: a call may only register while the closed bit is
 * 0. A drain close intent never decrements the count; only the wrapped listener's
 * terminal ({@link Registration#terminate()}) does. {@code drainedFuture}
 * completes once registration is closed and the open count reaches zero.
 */
public final class GrpcActiveCallRegistry {

    /** Minimal view of a drainable call so the registry is testable without a full ServerCall. */
    public interface ActiveCall {
        String fullMethodName();

        void closeForDrain(Status status);
    }

    /** Handle returned to the interceptor; its terminate() is driven by the wrapped listener. */
    public interface Registration {
        boolean isAccepted();

        void terminate();
    }

    private static final long CLOSED_BIT = 1L << 63;
    private static final long COUNT_MASK = ~CLOSED_BIT;

    private final AtomicLong state = new AtomicLong(0L);
    private final Map<Long, ActiveCall> live = new ConcurrentHashMap<>();
    private final AtomicLong idSeq = new AtomicLong();
    private final CompletableFuture<Void> drainedFuture = new CompletableFuture<>();

    private static boolean isClosed(long s) {
        return (s & CLOSED_BIT) != 0;
    }

    private static long count(long s) {
        return s & COUNT_MASK;
    }

    /**
     * Registers a non-unary call. If registration is already closed the call is
     * rejected (its interceptor should close it via the drain policy). Otherwise
     * the returned handle must be terminated exactly once by the wrapped listener.
     */
    public Registration register(ActiveCall call) {
        while (true) {
            long cur = state.get();
            if (isClosed(cur)) {
                return REJECTED;
            }
            long next = CLOSED_BIT & cur | (count(cur) + 1);
            if (state.compareAndSet(cur, next)) {
                long id = idSeq.incrementAndGet();
                live.put(id, call);
                return new AcceptedRegistration(id);
            }
        }
    }

    private void terminate(long id) {
        if (live.remove(id) == null) {
            return;
        }
        while (true) {
            long cur = state.get();
            long curCount = count(cur);
            if (curCount == 0) {
                throw new IllegalStateException("GrpcActiveCallRegistry open count underflow");
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
     * Closes the registration entry and sends one close intent to each live call
     * per the policy. New calls are rejected afterwards. If no calls are live,
     * completes {@code drainedFuture} immediately.
     */
    public void closeAll(GrpcDrainStatusPolicy policy) {
        while (true) {
            long cur = state.get();
            long next = cur | CLOSED_BIT;
            if (state.compareAndSet(cur, next)) {
                if (count(next) == 0) {
                    drainedFuture.complete(null);
                }
                break;
            }
        }
        for (ActiveCall call : live.values()) {
            try {
                call.closeForDrain(policy.closeStatusFor(call.fullMethodName()));
            } catch (Throwable ignored) {
                // best-effort close; the listener terminal still drives accounting
            }
        }
    }

    public boolean isRegistrationClosed() {
        return isClosed(state.get());
    }

    public long openCount() {
        return count(state.get());
    }

    public CompletableFuture<Void> drainedFuture() {
        return drainedFuture;
    }

    private final class AcceptedRegistration implements Registration {
        private final long id;

        AcceptedRegistration(long id) {
            this.id = id;
        }

        @Override
        public boolean isAccepted() {
            return true;
        }

        @Override
        public void terminate() {
            GrpcActiveCallRegistry.this.terminate(id);
        }
    }

    private static final Registration REJECTED = new Registration() {
        @Override
        public boolean isAccepted() {
            return false;
        }

        @Override
        public void terminate() {
            // no-op: a rejected call was never counted
        }
    };
}
