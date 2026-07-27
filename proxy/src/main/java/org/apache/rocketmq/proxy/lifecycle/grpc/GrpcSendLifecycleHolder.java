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

import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.proxy.lifecycle.ProtocolResult;
import org.apache.rocketmq.proxy.lifecycle.SendLifecycleContext;
import org.apache.rocketmq.proxy.lifecycle.SkipReason;

/**
 * Per-stream mutable holder created by the stream tracer (before interceptors
 * run) and injected into the gRPC {@code Context}. The interceptor later binds
 * exactly one permit; a late/gate-closed RPC instead records a synthetic skip.
 * The tracer's {@code streamClosed} writes the single canonical protocol
 * terminal. A permit may only be bound once.
 */
public final class GrpcSendLifecycleHolder {

    private final AtomicReference<SendLifecycleContext> permit = new AtomicReference<>();
    private volatile boolean rejectedBeforeAdmission;

    /** Binds the acquired permit exactly once. Returns false if already bound. */
    public boolean bindPermit(SendLifecycleContext context) {
        return permit.compareAndSet(null, context);
    }

    /** Records that this RPC never got a permit (late transport or closed gate). */
    public void rejectBeforeAdmission(SkipReason reason) {
        rejectedBeforeAdmission = true;
    }

    public boolean hasPermit() {
        return permit.get() != null;
    }

    public boolean wasRejectedBeforeAdmission() {
        return rejectedBeforeAdmission;
    }

    public SendLifecycleContext permit() {
        return permit.get();
    }

    /**
     * Canonical protocol terminal from the tracer's streamClosed. If a permit was
     * bound, records its protocol terminal; a bound permit that never started its
     * backend is skipped so it can still release.
     */
    public void onStreamClosed(boolean success, Throwable cause) {
        SendLifecycleContext context = permit.get();
        if (context == null) {
            return;
        }
        context.tryBackendSkipped(SkipReason.STREAM_CLOSED_BEFORE_DISPATCH);
        context.protocolTerminal(success ? ProtocolResult.success() : ProtocolResult.failure(cause));
    }
}
