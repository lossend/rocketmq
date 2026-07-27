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

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrpcActiveCallInterceptorTest {

    @SuppressWarnings("unchecked")
    private ServerCall<byte[], byte[]> mockCall(MethodDescriptor.MethodType type) {
        ServerCall<byte[], byte[]> call = mock(ServerCall.class);
        MethodDescriptor<byte[], byte[]> descriptor = mock(MethodDescriptor.class);
        when(descriptor.getFullMethodName()).thenReturn("apache.rocketmq.v2.MessagingService/Telemetry");
        when(descriptor.getType()).thenReturn(type);
        when(call.getMethodDescriptor()).thenReturn(descriptor);
        return call;
    }

    @SuppressWarnings("unchecked")
    private ServerCallHandler<byte[], byte[]> passThroughHandler() {
        ServerCallHandler<byte[], byte[]> handler = mock(ServerCallHandler.class);
        when(handler.startCall(any(), any())).thenReturn(new ServerCall.Listener<byte[]>() {
        });
        return handler;
    }

    @Test
    @DisplayName("a unary call is not registered")
    public void unaryNotRegistered() {
        GrpcActiveCallRegistry registry = new GrpcActiveCallRegistry();
        GrpcActiveCallInterceptor interceptor = new GrpcActiveCallInterceptor(registry);
        interceptor.interceptCall(mockCall(MethodDescriptor.MethodType.UNARY), new Metadata(),
            passThroughHandler());
        assertThat(registry.openCount()).isZero();
    }

    @Test
    @DisplayName("a bidi stream is registered and the wrapped listener terminal decrements on complete")
    public void bidiRegisteredAndTerminatedOnComplete() {
        GrpcActiveCallRegistry registry = new GrpcActiveCallRegistry();
        GrpcActiveCallInterceptor interceptor = new GrpcActiveCallInterceptor(registry);
        ServerCall.Listener<byte[]> listener = interceptor.interceptCall(
            mockCall(MethodDescriptor.MethodType.BIDI_STREAMING), new Metadata(), passThroughHandler());
        assertThat(registry.openCount()).isEqualTo(1);

        listener.onComplete();
        assertThat(registry.openCount()).isZero();
    }

    @Test
    @DisplayName("onCancel also decrements the registry exactly once")
    public void cancelTerminates() {
        GrpcActiveCallRegistry registry = new GrpcActiveCallRegistry();
        GrpcActiveCallInterceptor interceptor = new GrpcActiveCallInterceptor(registry);
        ServerCall.Listener<byte[]> listener = interceptor.interceptCall(
            mockCall(MethodDescriptor.MethodType.SERVER_STREAMING), new Metadata(), passThroughHandler());
        listener.onCancel();
        listener.onCancel();
        assertThat(registry.openCount()).isZero();
    }

    @Test
    @DisplayName("a synchronous startCall failure terminates the registration and rethrows")
    public void startCallFailureCompensates() {
        GrpcActiveCallRegistry registry = new GrpcActiveCallRegistry();
        GrpcActiveCallInterceptor interceptor = new GrpcActiveCallInterceptor(registry);
        @SuppressWarnings("unchecked")
        ServerCallHandler<byte[], byte[]> handler = mock(ServerCallHandler.class);
        when(handler.startCall(any(), any())).thenThrow(new RuntimeException("boom"));

        try {
            interceptor.interceptCall(mockCall(MethodDescriptor.MethodType.SERVER_STREAMING),
                new Metadata(), handler);
            org.junit.Assert.fail("expected the failure to propagate");
        } catch (RuntimeException expected) {
            assertThat(expected).hasMessage("boom");
        }
        assertThat(registry.openCount()).isZero();
    }
}
