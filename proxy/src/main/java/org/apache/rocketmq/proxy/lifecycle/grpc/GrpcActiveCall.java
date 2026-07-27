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

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps a non-unary {@link ServerCall} so drain can send one close intent while
 * business responses and the drain close serialize through the same lock. The
 * first close (business or drain) wins; later closes are idempotent no-ops. The
 * wrapper never marks the registry terminal itself — that is driven by the
 * wrapped listener's onComplete/onCancel.
 */
public final class GrpcActiveCall<T, R> extends ForwardingServerCall.SimpleForwardingServerCall<T, R>
    implements GrpcActiveCallRegistry.ActiveCall {

    private final String fullMethodName;
    private final Object lock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public GrpcActiveCall(ServerCall<T, R> delegate) {
        super(delegate);
        this.fullMethodName = delegate.getMethodDescriptor().getFullMethodName();
    }

    @Override
    public String fullMethodName() {
        return fullMethodName;
    }

    @Override
    public void sendHeaders(Metadata headers) {
        synchronized (lock) {
            if (!closed.get()) {
                super.sendHeaders(headers);
            }
        }
    }

    @Override
    public void sendMessage(R message) {
        synchronized (lock) {
            if (!closed.get()) {
                super.sendMessage(message);
            }
        }
    }

    @Override
    public void close(Status status, Metadata trailers) {
        synchronized (lock) {
            if (closed.compareAndSet(false, true)) {
                super.close(status, trailers);
            }
        }
    }

    @Override
    public void closeForDrain(Status status) {
        synchronized (lock) {
            if (closed.compareAndSet(false, true)) {
                super.close(status, new Metadata());
            }
        }
    }
}
