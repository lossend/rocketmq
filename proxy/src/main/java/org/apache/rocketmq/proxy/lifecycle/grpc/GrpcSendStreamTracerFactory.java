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

/**
 * Creates a per-stream {@link GrpcSendLifecycleHolder} for SendMessage RPCs before
 * interceptors run, injects it into the gRPC {@link Context} via
 * {@code filterContext}, and writes the canonical protocol terminal on
 * {@code streamClosed}. The holder is counted as an open send RPC from tracer
 * creation until stream close, so gate-rejected and sync-failed sends stay counted
 * through their real terminal. Non-send methods get a no-op tracer.
 */
public final class GrpcSendStreamTracerFactory extends ServerStreamTracer.Factory {

    public static final Context.Key<GrpcSendLifecycleHolder> HOLDER_KEY =
        Context.key("proxy-grpc-send-lifecycle-holder");

    private final String sendMethod;
    private final AtomicLong openSendRpcs;

    public GrpcSendStreamTracerFactory(AtomicLong openSendRpcs) {
        this(MessagingServiceGrpc.getSendMessageMethod().getFullMethodName(), openSendRpcs);
    }

    GrpcSendStreamTracerFactory(String sendMethod, AtomicLong openSendRpcs) {
        this.sendMethod = sendMethod;
        this.openSendRpcs = openSendRpcs;
    }

    @Override
    public ServerStreamTracer newServerStreamTracer(String fullMethodName, Metadata headers) {
        if (!sendMethod.equals(fullMethodName)) {
            return NoopTracer.INSTANCE;
        }
        openSendRpcs.incrementAndGet();
        return new SendTracer();
    }

    public long openSendRpcs() {
        return openSendRpcs.get();
    }

    private final class SendTracer extends ServerStreamTracer {
        private final GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
        private final AtomicLong closed = new AtomicLong();

        @Override
        public Context filterContext(Context context) {
            return context.withValue(HOLDER_KEY, holder);
        }

        @Override
        public void streamClosed(Status status) {
            if (closed.compareAndSet(0, 1)) {
                holder.onStreamClosed(status.isOk(), status.getCause());
                openSendRpcs.decrementAndGet();
            }
        }
    }

    private static final class NoopTracer extends ServerStreamTracer {
        static final NoopTracer INSTANCE = new NoopTracer();
    }
}
