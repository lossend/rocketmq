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

import io.grpc.Status;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GrpcActiveCallRegistryTest {

    private static final class RecordingCall implements GrpcActiveCallRegistry.ActiveCall {
        private final String method;
        final AtomicInteger closes = new AtomicInteger();
        volatile Status lastStatus;

        RecordingCall(String method) {
            this.method = method;
        }

        @Override
        public String fullMethodName() {
            return method;
        }

        @Override
        public void closeForDrain(Status status) {
            closes.incrementAndGet();
            lastStatus = status;
        }
    }

    @Test
    @DisplayName("an empty registry does not pre-complete its drained future")
    public void emptyRegistryNotPreCompleted() {
        GrpcActiveCallRegistry registry = new GrpcActiveCallRegistry();
        assertThat(registry.drainedFuture()).isNotCompleted();
        assertThat(registry.openCount()).isZero();
    }

    @Test
    @DisplayName("closeAll with zero live calls completes the drained future")
    public void closeAllWithZeroCompletes() {
        GrpcActiveCallRegistry registry = new GrpcActiveCallRegistry();
        registry.closeAll(new GrpcDrainStatusPolicy());
        assertThat(registry.drainedFuture()).isCompleted();
    }

    @Test
    @DisplayName("close intent does not decrement the open count; only listener terminal does")
    public void closeIntentDoesNotDecrement() {
        GrpcActiveCallRegistry registry = new GrpcActiveCallRegistry();
        RecordingCall call = new RecordingCall("apache.rocketmq.v2.MessagingService/Telemetry");
        GrpcActiveCallRegistry.Registration reg = registry.register(call);
        assertThat(reg.isAccepted()).isTrue();
        assertThat(registry.openCount()).isEqualTo(1);

        registry.closeAll(new GrpcDrainStatusPolicy());
        assertThat(call.closes.get()).isEqualTo(1);
        assertThat(registry.openCount()).isEqualTo(1);
        assertThat(registry.drainedFuture()).isNotCompleted();

        reg.terminate();
        assertThat(registry.openCount()).isZero();
        assertThat(registry.drainedFuture()).isCompleted();
    }

    @Test
    @DisplayName("terminate is idempotent and never underflows the open count")
    public void terminateIdempotent() {
        GrpcActiveCallRegistry registry = new GrpcActiveCallRegistry();
        GrpcActiveCallRegistry.Registration reg = registry.register(
            new RecordingCall("apache.rocketmq.v2.MessagingService/Telemetry"));
        reg.terminate();
        reg.terminate();
        assertThat(registry.openCount()).isZero();
    }

    @Test
    @DisplayName("registration after closeAll is rejected and not counted")
    public void registrationAfterCloseRejected() {
        GrpcActiveCallRegistry registry = new GrpcActiveCallRegistry();
        registry.closeAll(new GrpcDrainStatusPolicy());
        GrpcActiveCallRegistry.Registration reg = registry.register(
            new RecordingCall("apache.rocketmq.v2.MessagingService/ReceiveMessage"));
        assertThat(reg.isAccepted()).isFalse();
        assertThat(registry.openCount()).isZero();
    }

    @Test
    @DisplayName("thousands of register/terminate racing closeAll drain to zero exactly once")
    public void concurrentRegisterCloseRace() throws Exception {
        for (int round = 0; round < 20; round++) {
            final GrpcActiveCallRegistry registry = new GrpcActiveCallRegistry();
            final int threads = 32;
            final ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
            final CountDownLatch start = new CountDownLatch(1);
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    awaitLatch(start);
                    for (int j = 0; j < 20; j++) {
                        GrpcActiveCallRegistry.Registration reg = registry.register(
                            new RecordingCall("apache.rocketmq.v2.MessagingService/Telemetry"));
                        if (reg.isAccepted()) {
                            reg.terminate();
                        }
                    }
                });
            }
            pool.submit(() -> {
                awaitLatch(start);
                registry.closeAll(new GrpcDrainStatusPolicy());
            });
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            assertThat(registry.isRegistrationClosed()).isTrue();
            assertThat(registry.openCount()).isZero();
            assertThat(registry.drainedFuture()).isCompleted();
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
