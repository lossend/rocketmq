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

import io.grpc.Attributes;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.proxy.lifecycle.SendDrainGate;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrpcSendLifecycleInterceptorTest {

    private static final String SEND = "apache.rocketmq.v2.MessagingService/SendMessage";

    @SuppressWarnings("unchecked")
    private ServerCall<byte[], byte[]> mockCall(String method, boolean late) {
        ServerCall<byte[], byte[]> call = mock(ServerCall.class);
        MethodDescriptor<byte[], byte[]> descriptor = mock(MethodDescriptor.class);
        when(descriptor.getFullMethodName()).thenReturn(method);
        when(call.getMethodDescriptor()).thenReturn(descriptor);
        Attributes attrs = Attributes.newBuilder()
            .set(GrpcTransportLifecycleFilter.LATE_AFTER_LB_CUTOFF, late)
            .build();
        when(call.getAttributes()).thenReturn(attrs);
        return call;
    }

    @SuppressWarnings("unchecked")
    private ServerCallHandler<byte[], byte[]> handlerThatCounts(AtomicInteger started) {
        ServerCallHandler<byte[], byte[]> handler = mock(ServerCallHandler.class);
        when(handler.startCall(any(), any())).thenAnswer(inv -> {
            started.incrementAndGet();
            return new ServerCall.Listener<byte[]>() {
            };
        });
        return handler;
    }

    private <T> T runInHolderContext(GrpcSendLifecycleHolder holder, java.util.function.Supplier<T> body) {
        Context ctx = Context.ROOT.withValue(GrpcSendStreamTracerFactory.HOLDER_KEY, holder);
        Context previous = ctx.attach();
        try {
            return body.get();
        } finally {
            ctx.detach(previous);
        }
    }

    @Test
    @DisplayName("a normal send acquires a permit, binds it to the holder, and invokes the handler")
    public void normalSendBindsAndDispatches() {
        SendDrainGate gate = new SendDrainGate();
        GrpcSendLifecycleInterceptor interceptor = new GrpcSendLifecycleInterceptor(SEND, gate);
        GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
        AtomicInteger started = new AtomicInteger();

        runInHolderContext(holder, () ->
            interceptor.interceptCall(mockCall(SEND, false), new Metadata(), handlerThatCounts(started)));

        assertThat(started.get()).isEqualTo(1);
        assertThat(holder.hasPermit()).isTrue();
        assertThat(gate.acceptedCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("when the gate is closed the send is rejected with UNAVAILABLE and never dispatched")
    public void closedGateRejects() {
        SendDrainGate gate = new SendDrainGate();
        gate.closeAdmission();
        GrpcSendLifecycleInterceptor interceptor = new GrpcSendLifecycleInterceptor(SEND, gate);
        GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
        AtomicInteger started = new AtomicInteger();
        ServerCall<byte[], byte[]> call = mockCall(SEND, false);
        AtomicReference<Status> closed = new AtomicReference<>();
        org.mockito.Mockito.doAnswer(inv -> {
            closed.set(inv.getArgument(0));
            return null;
        }).when(call).close(any(), any());

        runInHolderContext(holder, () ->
            interceptor.interceptCall(call, new Metadata(), handlerThatCounts(started)));

        assertThat(started.get()).isZero();
        assertThat(holder.wasRejectedBeforeAdmission()).isTrue();
        assertThat(closed.get().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    }

    @Test
    @DisplayName("a late transport is rejected with UNAVAILABLE without acquiring a permit")
    public void lateTransportRejected() {
        SendDrainGate gate = new SendDrainGate();
        GrpcSendLifecycleInterceptor interceptor = new GrpcSendLifecycleInterceptor(SEND, gate);
        GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
        AtomicInteger started = new AtomicInteger();

        runInHolderContext(holder, () ->
            interceptor.interceptCall(mockCall(SEND, true), new Metadata(), handlerThatCounts(started)));

        assertThat(started.get()).isZero();
        assertThat(gate.acceptedCount()).isZero();
        assertThat(holder.wasRejectedBeforeAdmission()).isTrue();
    }

    @Test
    @DisplayName("a non-send method passes straight through to the handler")
    public void nonSendPassesThrough() {
        SendDrainGate gate = new SendDrainGate();
        GrpcSendLifecycleInterceptor interceptor = new GrpcSendLifecycleInterceptor(SEND, gate);
        AtomicInteger started = new AtomicInteger();
        interceptor.interceptCall(mockCall("apache.rocketmq.v2.MessagingService/Telemetry", false),
            new Metadata(), handlerThatCounts(started));
        assertThat(started.get()).isEqualTo(1);
    }
}
