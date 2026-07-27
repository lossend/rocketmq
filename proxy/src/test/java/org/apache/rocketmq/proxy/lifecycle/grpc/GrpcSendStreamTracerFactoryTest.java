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
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.ServerStreamTracer;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GrpcSendStreamTracerFactoryTest {

    private final String sendMethod = MessagingServiceGrpc.getSendMessageMethod().getFullMethodName();
    private final String telemetryMethod = MessagingServiceGrpc.getTelemetryMethod().getFullMethodName();

    @Test
    @DisplayName("a SendMessage stream increments open RPCs and injects the holder into the context")
    public void sendStreamCountsAndInjectsHolder() {
        AtomicLong open = new AtomicLong();
        GrpcSendStreamTracerFactory factory = new GrpcSendStreamTracerFactory(open);
        ServerStreamTracer tracer = factory.newServerStreamTracer(sendMethod, new Metadata());
        assertThat(open.get()).isEqualTo(1);

        Context filtered = tracer.filterContext(Context.ROOT);
        assertThat(GrpcSendStreamTracerFactory.HOLDER_KEY.get(filtered)).isNotNull();
    }

    @Test
    @DisplayName("a non-send method gets a no-op tracer that does not touch the counter")
    public void nonSendMethodNoop() {
        AtomicLong open = new AtomicLong();
        GrpcSendStreamTracerFactory factory = new GrpcSendStreamTracerFactory(open);
        ServerStreamTracer tracer = factory.newServerStreamTracer(telemetryMethod, new Metadata());
        assertThat(open.get()).isZero();
        // no-op tracer must not carry the holder
        Context filtered = tracer.filterContext(Context.ROOT);
        assertThat(GrpcSendStreamTracerFactory.HOLDER_KEY.get(filtered)).isNull();
    }

    @Test
    @DisplayName("streamClosed decrements the open RPC count exactly once")
    public void streamClosedDecrementsOnce() {
        AtomicLong open = new AtomicLong();
        GrpcSendStreamTracerFactory factory = new GrpcSendStreamTracerFactory(open);
        ServerStreamTracer tracer = factory.newServerStreamTracer(sendMethod, new Metadata());
        assertThat(open.get()).isEqualTo(1);
        tracer.streamClosed(Status.OK);
        tracer.streamClosed(Status.OK);
        assertThat(open.get()).isZero();
    }
}
