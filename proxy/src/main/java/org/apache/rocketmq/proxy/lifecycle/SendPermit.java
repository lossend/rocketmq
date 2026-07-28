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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A single accepted send's dual-terminal permit. Backend state, protocol
 * terminal, and the released bit are packed into one {@link AtomicInteger} so a
 * single CAS linearizes every transition. The permit releases exactly once,
 * when backend is {TERMINAL|SKIPPED} and protocol is TERMINAL, and only that
 * winning CAS decrements the owning gate.
 */
public final class SendPermit implements SendLifecycleContext {

    // backend: bits 0-1
    private static final int BACKEND_MASK = 0b11;
    private static final int BACKEND_NOT_STARTED = 0;
    private static final int BACKEND_STARTED = 1;
    private static final int BACKEND_TERMINAL = 2;
    private static final int BACKEND_SKIPPED = 3;
    // protocol: bit 2
    private static final int PROTOCOL_TERMINAL = 1 << 2;
    // released: bit 3
    private static final int RELEASED = 1 << 3;

    private final SendProtocol protocol;
    private final AtomicInteger state = new AtomicInteger(BACKEND_NOT_STARTED);
    private final CompletableFuture<Void> released = new CompletableFuture<>();
    private final Runnable onRelease;

    SendPermit(SendProtocol protocol, Runnable onRelease) {
        this.protocol = protocol;
        this.onRelease = onRelease;
    }

    public SendProtocol protocol() {
        return protocol;
    }

    private static int backend(int s) {
        return s & BACKEND_MASK;
    }

    private static boolean protocolTerminalBit(int s) {
        return (s & PROTOCOL_TERMINAL) != 0;
    }

    private static boolean backendComplete(int s) {
        int b = backend(s);
        return b == BACKEND_TERMINAL || b == BACKEND_SKIPPED;
    }

    @Override
    public boolean backendStarted() {
        while (true) {
            int cur = state.get();
            if (backend(cur) != BACKEND_NOT_STARTED) {
                return false;
            }
            int next = (cur & ~BACKEND_MASK) | BACKEND_STARTED;
            if (state.compareAndSet(cur, next)) {
                return true;
            }
        }
    }

    @Override
    public void backendTerminal(Throwable cause) {
        while (true) {
            int cur = state.get();
            int b = backend(cur);
            if (b == BACKEND_TERMINAL) {
                return;
            }
            if (b != BACKEND_STARTED) {
                throw new IllegalStateException(
                    "backendTerminal is only valid from STARTED, current backend=" + b);
            }
            int next = (cur & ~BACKEND_MASK) | BACKEND_TERMINAL;
            if (state.compareAndSet(cur, next)) {
                maybeRelease(next);
                return;
            }
        }
    }

    @Override
    public boolean tryBackendSkipped(SkipReason reason) {
        while (true) {
            int cur = state.get();
            if (backend(cur) != BACKEND_NOT_STARTED) {
                return false;
            }
            int next = (cur & ~BACKEND_MASK) | BACKEND_SKIPPED;
            if (state.compareAndSet(cur, next)) {
                maybeRelease(next);
                return true;
            }
        }
    }

    @Override
    public void protocolTerminal(ProtocolResult result) {
        while (true) {
            int cur = state.get();
            if (protocolTerminalBit(cur)) {
                return;
            }
            int next = cur | PROTOCOL_TERMINAL;
            if (state.compareAndSet(cur, next)) {
                maybeRelease(next);
                return;
            }
        }
    }

    private void maybeRelease(int observed) {
        if (!(backendComplete(observed) && protocolTerminalBit(observed))) {
            return;
        }
        while (true) {
            int cur = state.get();
            if ((cur & RELEASED) != 0) {
                return;
            }
            if (!(backendComplete(cur) && protocolTerminalBit(cur))) {
                return;
            }
            int next = cur | RELEASED;
            if (state.compareAndSet(cur, next)) {
                if (onRelease != null) {
                    onRelease.run();
                }
                released.complete(null);
                return;
            }
        }
    }

    @Override
    public CompletableFuture<Void> releasedFuture() {
        return released;
    }

    @Deprecated
    @Override
    public CompletableFuture<Void> completionFuture() {
        return releasedFuture();
    }
}
