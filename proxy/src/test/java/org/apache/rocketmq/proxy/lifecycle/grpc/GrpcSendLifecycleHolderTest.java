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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.proxy.lifecycle.ProtocolResult;
import org.apache.rocketmq.proxy.lifecycle.SendDrainGate;
import org.apache.rocketmq.proxy.lifecycle.SendLifecycleContext;
import org.apache.rocketmq.proxy.lifecycle.SendPermit;
import org.apache.rocketmq.proxy.lifecycle.SendProtocol;
import org.apache.rocketmq.proxy.lifecycle.SkipReason;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GrpcSendLifecycleHolderTest {

    @Test
    @DisplayName("a permit binds exactly once")
    public void bindOnce() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit permit = gate.tryAcquire(SendProtocol.GRPC).get();
        GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
        assertThat(holder.bindPermit(permit)).isTrue();
        assertThat(holder.bindPermit(permit)).isFalse();
        assertThat(holder.hasPermit()).isTrue();
    }

    @Test
    @DisplayName("streamClosed on a bound-but-never-started permit skips the backend and releases it")
    public void streamClosedSkipsUnstartedBackend() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit permit = gate.tryAcquire(SendProtocol.GRPC).get();
        GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
        holder.bindPermit(permit);

        holder.onStreamClosed(true, null);
        assertThat(permit.releasedFuture()).isCompleted();
        assertThat(gate.acceptedCount()).isZero();
    }

    @Test
    @DisplayName("streamClosed before permit binding is latched and delivered after bind")
    public void streamClosedBeforeBindIsDeliveredAfterBind() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit permit = gate.tryAcquire(SendProtocol.GRPC).get();
        GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();

        holder.onStreamClosed(true, null);
        assertThat(permit.releasedFuture()).isNotCompleted();

        assertThat(holder.bindPermit(permit)).isTrue();
        assertThat(permit.releasedFuture()).isCompleted();
        assertThat(gate.acceptedCount()).isZero();
    }

    @Test
    @DisplayName("only the first stream terminal is retained before permit binding")
    public void firstStreamTerminalWinsBeforeBind() {
        GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
        RecordingLifecycleContext context = new RecordingLifecycleContext();
        RuntimeException failure = new RuntimeException("first close");

        holder.onStreamClosed(false, failure);
        holder.onStreamClosed(true, null);
        assertThat(holder.bindPermit(context)).isTrue();

        assertThat(context.backendSkippedCalls.get()).isOne();
        assertThat(context.protocolTerminalCalls.get()).isOne();
        assertThat(context.protocolResult.isSuccess()).isFalse();
        assertThat(context.protocolResult.cause()).isSameAs(failure);
    }

    @Test
    @DisplayName("concurrent bind and stream close deliver the terminal exactly once")
    public void concurrentBindAndStreamCloseDeliverTerminalExactlyOnce() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 100; i++) {
                GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
                RecordingLifecycleContext context = new RecordingLifecycleContext();
                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch start = new CountDownLatch(1);

                Future<Boolean> bind = executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    return holder.bindPermit(context);
                });
                Future<?> close = executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    holder.onStreamClosed(true, null);
                    return null;
                });

                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                assertThat(bind.get(10, TimeUnit.SECONDS)).isTrue();
                close.get(10, TimeUnit.SECONDS);

                holder.onStreamClosed(false, new RuntimeException("duplicate close"));
                assertThat(context.backendSkippedCalls.get()).isOne();
                assertThat(context.protocolTerminalCalls.get()).isOne();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("streamClosed after a started+terminal backend writes only the protocol terminal")
    public void streamClosedAfterBackendTerminal() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit permit = gate.tryAcquire(SendProtocol.GRPC).get();
        GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
        holder.bindPermit(permit);

        assertThat(permit.backendStarted()).isTrue();
        permit.backendTerminal(null);
        assertThat(permit.releasedFuture()).isNotCompleted();

        holder.onStreamClosed(true, null);
        assertThat(permit.releasedFuture()).isCompleted();
    }

    @Test
    @DisplayName("a rejected-before-admission holder has no permit and streamClosed is a no-op")
    public void rejectedBeforeAdmission() {
        GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
        holder.rejectBeforeAdmission(SkipReason.GATE_CLOSED);
        assertThat(holder.hasPermit()).isFalse();
        assertThat(holder.wasRejectedBeforeAdmission()).isTrue();
        holder.onStreamClosed(false, new RuntimeException("closed"));
    }

    private static final class RecordingLifecycleContext implements SendLifecycleContext {
        private final AtomicInteger backendSkippedCalls = new AtomicInteger();
        private final AtomicInteger protocolTerminalCalls = new AtomicInteger();
        private final CompletableFuture<Void> released = new CompletableFuture<>();
        private volatile ProtocolResult protocolResult;

        @Override
        public boolean backendStarted() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void backendTerminal(Throwable cause) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryBackendSkipped(SkipReason reason) {
            backendSkippedCalls.incrementAndGet();
            return true;
        }

        @Override
        public void protocolTerminal(ProtocolResult result) {
            protocolResult = result;
            protocolTerminalCalls.incrementAndGet();
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
}
