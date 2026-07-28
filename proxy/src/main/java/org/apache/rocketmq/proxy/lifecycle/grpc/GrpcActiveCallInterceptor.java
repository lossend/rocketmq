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

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * Registers every non-unary RPC with the {@link GrpcActiveCallRegistry} before
 * the handler starts, so even a Telemetry stream that has not yet sent its first
 * SETTINGS is covered. Unary calls pass through untouched. The wrapped listener's
 * onComplete/onCancel is the single terminal that decrements the registry; a drain
 * close intent alone never counts as terminal.
 */
public final class GrpcActiveCallInterceptor implements ServerInterceptor {

    private final GrpcActiveCallRegistry registry;
    private final GrpcDrainStatusPolicy drainStatusPolicy;

    public GrpcActiveCallInterceptor(GrpcActiveCallRegistry registry) {
        this(registry, new GrpcDrainStatusPolicy());
    }

    public GrpcActiveCallInterceptor(GrpcActiveCallRegistry registry,
        GrpcDrainStatusPolicy drainStatusPolicy) {
        this.registry = registry;
        this.drainStatusPolicy = drainStatusPolicy;
    }

    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(ServerCall<T, R> call,
        Metadata headers, ServerCallHandler<T, R> next) {
        if (call.getMethodDescriptor().getType() == MethodDescriptor.MethodType.UNARY) {
            return next.startCall(call, headers);
        }

        GrpcActiveCall<T, R> wrapped = new GrpcActiveCall<>(call);
        GrpcActiveCallRegistry.Registration registration = registry.register(wrapped);
        if (!registration.isAccepted()) {
            wrapped.closeForDrain(drainStatusPolicy.closeStatusFor(wrapped.fullMethodName()));
            return new ServerCall.Listener<T>() {
            };
        }
        ServerCall.Listener<T> listener;
        try {
            listener = next.startCall(wrapped, headers);
        } catch (RuntimeException | Error startFailure) {
            registration.terminate();
            throw startFailure;
        }
        return new TerminatingListener<>(listener, registration);
    }

    private static final class TerminatingListener<T>
        extends ForwardingServerCallListener.SimpleForwardingServerCallListener<T> {

        private final GrpcActiveCallRegistry.Registration registration;

        TerminatingListener(ServerCall.Listener<T> delegate,
            GrpcActiveCallRegistry.Registration registration) {
            super(delegate);
            this.registration = registration;
        }

        @Override
        public void onComplete() {
            try {
                super.onComplete();
            } finally {
                registration.terminate();
            }
        }

        @Override
        public void onCancel() {
            try {
                super.onCancel();
            } finally {
                registration.terminate();
            }
        }
    }
}
