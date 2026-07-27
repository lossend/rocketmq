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

import apache.rocketmq.v2.MessagingServiceGrpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Optional;
import org.apache.rocketmq.proxy.lifecycle.SendDrainGate;
import org.apache.rocketmq.proxy.lifecycle.SendLifecycleContext;
import org.apache.rocketmq.proxy.lifecycle.SendProtocol;
import org.apache.rocketmq.proxy.lifecycle.SkipReason;

/**
 * For SendMessage RPCs, acquires a gate permit and CAS-binds it to the tracer's
 * holder from the current {@link io.grpc.Context}. A late transport (tagged by
 * {@link GrpcTransportLifecycleFilter}) or a closed gate is closed with
 * {@code UNAVAILABLE} and the handler is never invoked. A missing/duplicate
 * holder is an invariant violation, closed with {@code INTERNAL}.
 */
public final class GrpcSendLifecycleInterceptor implements ServerInterceptor {

    private final String sendMethod;
    private final SendDrainGate gate;

    public GrpcSendLifecycleInterceptor(SendDrainGate gate) {
        this(MessagingServiceGrpc.getSendMessageMethod().getFullMethodName(), gate);
    }

    GrpcSendLifecycleInterceptor(String sendMethod, SendDrainGate gate) {
        this.sendMethod = sendMethod;
        this.gate = gate;
    }

    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(ServerCall<T, R> call,
        Metadata headers, ServerCallHandler<T, R> next) {
        if (!sendMethod.equals(call.getMethodDescriptor().getFullMethodName())) {
            return next.startCall(call, headers);
        }

        GrpcSendLifecycleHolder holder = GrpcSendStreamTracerFactory.HOLDER_KEY.get();
        if (holder == null) {
            call.close(Status.INTERNAL.withDescription("missing send lifecycle holder"), new Metadata());
            return new ServerCall.Listener<T>() {
            };
        }

        Boolean late = call.getAttributes().get(GrpcTransportLifecycleFilter.LATE_AFTER_LB_CUTOFF);
        if (Boolean.TRUE.equals(late)) {
            holder.rejectBeforeAdmission(SkipReason.LATE_TRANSPORT);
            call.close(Status.UNAVAILABLE.withDescription(GrpcDrainStatusPolicy.DRAINING_DESCRIPTION),
                new Metadata());
            return new ServerCall.Listener<T>() {
            };
        }

        Optional<? extends SendLifecycleContext> permit = gate.tryAcquire(SendProtocol.GRPC);
        if (!permit.isPresent()) {
            holder.rejectBeforeAdmission(SkipReason.GATE_CLOSED);
            call.close(Status.UNAVAILABLE.withDescription(GrpcDrainStatusPolicy.DRAINING_DESCRIPTION),
                new Metadata());
            return new ServerCall.Listener<T>() {
            };
        }

        if (!holder.bindPermit(permit.get())) {
            // duplicate binding: release this permit's accounting and fail loudly
            permit.get().tryBackendSkipped(SkipReason.SYNC_VALIDATION);
            permit.get().protocolTerminal(org.apache.rocketmq.proxy.lifecycle.ProtocolResult.failure(
                new IllegalStateException("duplicate holder bind")));
            call.close(Status.INTERNAL.withDescription("duplicate send lifecycle binding"), new Metadata());
            return new ServerCall.Listener<T>() {
            };
        }

        return next.startCall(call, headers);
    }
}
