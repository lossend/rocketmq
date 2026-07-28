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
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.proxy.lifecycle.SendDrainGate;
import org.junit.After;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the fallback that the send-drain accounting relies on: grpc-java invokes
 * {@code ServerStreamTracer.streamClosed} for every terminated stream, so a send
 * whose backend never started still has its permit released.
 * <p>
 * This exercises a real in-process gRPC server with the production tracer factory,
 * send interceptor and gate. It does not construct the full
 * {@code GrpcMessagingApplication}; the service stands in for the paths that finish
 * an RPC without ever dispatching to the Broker (synchronous validation failure,
 * executor rejection).
 */
public class GrpcStreamClosedFallbackTest {

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

    private MessagingServiceGrpc.MessagingServiceBlockingStub start(
        SendDrainGate gate, AtomicLong openSendRpcs, MessagingServiceGrpc.MessagingServiceImplBase service)
        throws Exception {
        String name = "fallback-" + UUID.randomUUID();
        server = InProcessServerBuilder.forName(name)
            .directExecutor()
            .addService(service)
            .addStreamTracerFactory(new GrpcSendStreamTracerFactory(openSendRpcs))
            .intercept(new GrpcSendLifecycleInterceptor(gate))
            .build()
            .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        return MessagingServiceGrpc.newBlockingStub(channel);
    }

    @Test
    @DisplayName("a send that fails before dispatch still releases its permit when the stream closes")
    public void permitReleasedWhenBackendNeverStarted() throws Exception {
        SendDrainGate gate = new SendDrainGate();
        AtomicLong openSendRpcs = new AtomicLong();
        // Stands in for the synchronous-validation failure path: the RPC is completed
        // with an error response and the Broker is never called.
        MessagingServiceGrpc.MessagingServiceImplBase service =
            new MessagingServiceGrpc.MessagingServiceImplBase() {
                @Override
                public void sendMessage(SendMessageRequest request,
                    StreamObserver<SendMessageResponse> responseObserver) {
                    responseObserver.onNext(SendMessageResponse.newBuilder().build());
                    responseObserver.onCompleted();
                }
            };

        MessagingServiceGrpc.MessagingServiceBlockingStub stub = start(gate, openSendRpcs, service);
        stub.sendMessage(SendMessageRequest.newBuilder().build());

        // The permit was admitted, then released purely by the streamClosed fallback.
        assertThat(gate.acceptedCount()).isZero();
        assertThat(openSendRpcs.get()).isZero();
        // Admission is still open: this was a normal RPC, not a drain.
        assertThat(gate.isAdmissionClosed()).isFalse();
    }

    @Test
    @DisplayName("a send whose handler throws also releases its permit when the stream closes")
    public void permitReleasedWhenHandlerThrows() throws Exception {
        SendDrainGate gate = new SendDrainGate();
        AtomicLong openSendRpcs = new AtomicLong();
        MessagingServiceGrpc.MessagingServiceImplBase service =
            new MessagingServiceGrpc.MessagingServiceImplBase() {
                @Override
                public void sendMessage(SendMessageRequest request,
                    StreamObserver<SendMessageResponse> responseObserver) {
                    throw new IllegalStateException("synchronous failure before dispatch");
                }
            };

        MessagingServiceGrpc.MessagingServiceBlockingStub stub = start(gate, openSendRpcs, service);
        try {
            stub.sendMessage(SendMessageRequest.newBuilder().build());
        } catch (StatusRuntimeException expected) {
            // grpc turns the thrown exception into an UNKNOWN status for the caller
        }

        assertThat(gate.acceptedCount()).isZero();
        assertThat(openSendRpcs.get()).isZero();
    }

    @Test
    @DisplayName("a closed gate rejects the send without leaking accounting")
    public void closedGateLeavesNoResidue() throws Exception {
        SendDrainGate gate = new SendDrainGate();
        AtomicLong openSendRpcs = new AtomicLong();
        gate.closeAdmission();
        MessagingServiceGrpc.MessagingServiceImplBase service =
            new MessagingServiceGrpc.MessagingServiceImplBase() {
                @Override
                public void sendMessage(SendMessageRequest request,
                    StreamObserver<SendMessageResponse> responseObserver) {
                    responseObserver.onNext(SendMessageResponse.newBuilder().build());
                    responseObserver.onCompleted();
                }
            };

        MessagingServiceGrpc.MessagingServiceBlockingStub stub = start(gate, openSendRpcs, service);
        try {
            stub.sendMessage(SendMessageRequest.newBuilder().build());
        } catch (StatusRuntimeException expected) {
            assertThat(expected.getStatus().getDescription())
                .isEqualTo(GrpcDrainStatusPolicy.DRAINING_DESCRIPTION);
        }

        assertThat(gate.acceptedCount()).isZero();
        assertThat(openSendRpcs.get()).isZero();
    }
}
