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

package org.apache.rocketmq.proxy.grpc;

import io.grpc.ServerInterceptor;
import io.grpc.ServerStreamTracer;
import io.grpc.ServerTransportFilter;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class GrpcServerBuilderLifecycleTest {

    @Test
    @DisplayName("send-drain off keeps transport and active-call lifecycle but skips send accounting")
    public void sendDrainOffInstallsOnlyTransportLifecycle() {
        NettyServerBuilder netty = mock(NettyServerBuilder.class, RETURNS_SELF);
        ServerStreamTracer.Factory tracer = mock(ServerStreamTracer.Factory.class);
        ServerInterceptor send = mock(ServerInterceptor.class);
        ServerInterceptor activeCall = mock(ServerInterceptor.class);
        ServerTransportFilter transport = mock(ServerTransportFilter.class);

        new GrpcServerBuilder(netty).configLifecycle(
            tracer, send, activeCall, transport, 300, 30, false);

        verify(netty).maxConnectionAge(300, TimeUnit.SECONDS);
        verify(netty).maxConnectionAgeGrace(30, TimeUnit.SECONDS);
        verify(netty).addTransportFilter(transport);
        verify(netty).intercept(activeCall);
        verify(netty, never()).addStreamTracerFactory(tracer);
        verify(netty, never()).intercept(send);
    }

    @Test
    @DisplayName("send-drain on installs tracer and send interceptor with the transport lifecycle")
    public void sendDrainOnInstallsCompleteLifecycle() {
        NettyServerBuilder netty = mock(NettyServerBuilder.class, RETURNS_SELF);
        ServerStreamTracer.Factory tracer = mock(ServerStreamTracer.Factory.class);
        ServerInterceptor send = mock(ServerInterceptor.class);
        ServerInterceptor activeCall = mock(ServerInterceptor.class);
        ServerTransportFilter transport = mock(ServerTransportFilter.class);

        new GrpcServerBuilder(netty).configLifecycle(
            tracer, send, activeCall, transport, 300, 30, true);

        verify(netty).addTransportFilter(transport);
        verify(netty).intercept(activeCall);
        verify(netty).addStreamTracerFactory(tracer);
        verify(netty).intercept(send);
    }

    @Test
    @DisplayName("the original six-argument API preserves send-drain installation")
    public void legacyOverloadKeepsSendDrainEnabled() {
        NettyServerBuilder netty = mock(NettyServerBuilder.class, RETURNS_SELF);
        ServerStreamTracer.Factory tracer = mock(ServerStreamTracer.Factory.class);
        ServerInterceptor send = mock(ServerInterceptor.class);
        ServerInterceptor activeCall = mock(ServerInterceptor.class);
        ServerTransportFilter transport = mock(ServerTransportFilter.class);

        new GrpcServerBuilder(netty).configLifecycle(
            tracer, send, activeCall, transport, 300, 30);

        verify(netty).addStreamTracerFactory(tracer);
        verify(netty).intercept(send);
    }
}
