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

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.MessagingServiceGrpc;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.lifecycle.SendDrainGate;
import org.junit.After;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the send-drain-disabled shape: when the tracer factory and send-permit
 * interceptor are not installed (as {@code GrpcServerBuilder.configLifecycle} does
 * when {@code installSendDrain} is false), no permit is ever acquired, the send
 * still succeeds, and the gate drains immediately once admission is closed — so the
 * coordinator's {@code drainedFuture} does not hang.
 */
public class SendDrainDisabledTest {

    private Server server;
    private ManagedChannel channel;

    @After
    public void tearDown() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
    }

    private MessagingServiceGrpc.MessagingServiceBlockingStub startWithoutSendDrain(
        MessagingServiceGrpc.MessagingServiceImplBase service) throws Exception {
        String name = "no-send-drain-" + UUID.randomUUID();
        // Deliberately no addStreamTracerFactory / no send interceptor, mirroring
        // configLifecycle(..., installSendDrain=false).
        server = InProcessServerBuilder.forName(name)
            .directExecutor()
            .addService(service)
            .build()
            .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        return MessagingServiceGrpc.newBlockingStub(channel);
    }

    @Test
    @DisplayName("with send-drain off, a send never touches the gate but still succeeds")
    public void sendSucceedsWithoutTouchingGate() throws Exception {
        SendDrainGate gate = new SendDrainGate();
        AtomicBoolean brokerCalled = new AtomicBoolean();
        MessagingServiceGrpc.MessagingServiceImplBase service =
            new MessagingServiceGrpc.MessagingServiceImplBase() {
                @Override
                public void sendMessage(SendMessageRequest request,
                    StreamObserver<SendMessageResponse> responseObserver) {
                    // This handler stands in for the normal Broker-backed activity.
                    brokerCalled.set(true);
                    responseObserver.onNext(SendMessageResponse.newBuilder()
                        .setStatus(ResponseBuilder.getInstance().buildStatus(
                            Code.OK, "Broker returned SEND_OK"))
                        .build());
                    responseObserver.onCompleted();
                }
            };

        MessagingServiceGrpc.MessagingServiceBlockingStub stub = startWithoutSendDrain(service);
        SendMessageResponse response = stub.sendMessage(SendMessageRequest.newBuilder().build());

        assertThat(brokerCalled).isTrue();
        assertThat(response.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(gate.acceptedCount()).isZero();
    }

    @Test
    @DisplayName("with send-drain off, closing admission drains the gate immediately")
    public void closeAdmissionDrainsImmediately() {
        SendDrainGate gate = new SendDrainGate();
        gate.closeAdmission();
        // No permits were ever acquired, so the drain must already be complete.
        assertThat(gate.drainedFuture()).isCompleted();
    }
}
