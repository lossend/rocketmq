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

import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingActivity;
import org.apache.rocketmq.proxy.lifecycle.ProtocolResult;
import org.apache.rocketmq.proxy.lifecycle.SendDrainGate;
import org.apache.rocketmq.proxy.lifecycle.SendLifecycleContext;
import org.apache.rocketmq.proxy.lifecycle.SendPermit;
import org.apache.rocketmq.proxy.lifecycle.SendProtocol;
import org.apache.rocketmq.proxy.lifecycle.SkipReason;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SendLifecycleMessagingActivityTest {

    private static final SendMessageRequest REQUEST = SendMessageRequest.getDefaultInstance();
    private static final SendMessageResponse RESPONSE = SendMessageResponse.getDefaultInstance();

    @Test
    @DisplayName("the response-visible future waits until backendTerminal has returned")
    public void responseFutureIncludesBackendTerminalStage() throws Exception {
        GrpcMessagingActivity delegate = mock(GrpcMessagingActivity.class);
        SendLifecycleContext lifecycle = mock(SendLifecycleContext.class);
        CountDownLatch terminalEntered = new CountDownLatch(1);
        CountDownLatch allowTerminal = new CountDownLatch(1);
        when(lifecycle.backendStarted()).thenReturn(true);
        doAnswer(invocation -> {
            terminalEntered.countDown();
            if (!allowTerminal.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting to finish backend terminal");
            }
            return null;
        }).when(lifecycle).backendTerminal(any());

        CompletableFuture<SendMessageResponse> brokerFuture = new CompletableFuture<>();
        when(delegate.sendMessage(any(), any())).thenReturn(brokerFuture);
        SendLifecycleMessagingActivity activity = new SendLifecycleMessagingActivity(delegate);
        CompletableFuture<SendMessageResponse> responseFuture =
            activity.sendMessage(ProxyContext.create().setSendLifecycleContext(lifecycle), REQUEST);

        Thread completer = new Thread(() -> brokerFuture.complete(RESPONSE), "broker-future-completer");
        completer.start();
        try {
            assertThat(terminalEntered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(responseFuture).isNotCompleted();
        } finally {
            allowTerminal.countDown();
            completer.join(TimeUnit.SECONDS.toMillis(10));
        }

        assertThat(responseFuture).isCompletedWithValue(RESPONSE);
        verify(lifecycle).backendTerminal(null);
    }

    @Test
    @DisplayName("a backendTerminal failure makes the response-visible future fail")
    public void backendTerminalFailureFailsResponseFuture() {
        GrpcMessagingActivity delegate = mock(GrpcMessagingActivity.class);
        SendLifecycleContext lifecycle = mock(SendLifecycleContext.class);
        when(lifecycle.backendStarted()).thenReturn(true);
        doThrow(new IllegalStateException("terminal bookkeeping failed"))
            .when(lifecycle).backendTerminal(null);
        when(delegate.sendMessage(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(RESPONSE));

        CompletableFuture<SendMessageResponse> responseFuture =
            new SendLifecycleMessagingActivity(delegate).sendMessage(
                ProxyContext.create().setSendLifecycleContext(lifecycle), REQUEST);

        assertThat(responseFuture).isCompletedExceptionally();
        verify(lifecycle).backendTerminal(null);
    }

    @Test
    @DisplayName("a backend skipped before dispatch never calls the Broker and fails explicitly")
    public void skippedBackendFailsWithoutBrokerCall() {
        GrpcMessagingActivity delegate = mock(GrpcMessagingActivity.class);
        SendDrainGate gate = new SendDrainGate();
        SendPermit permit = gate.tryAcquire(SendProtocol.GRPC).get();
        assertThat(permit.tryBackendSkipped(SkipReason.EXECUTOR_REJECTED)).isTrue();

        SendLifecycleMessagingActivity activity = new SendLifecycleMessagingActivity(delegate);
        CompletableFuture<SendMessageResponse> responseFuture =
            activity.sendMessage(ProxyContext.create().setSendLifecycleContext(permit), REQUEST);

        assertThat(responseFuture).isCompletedExceptionally();
        verify(delegate, never()).sendMessage(any(), any());
        permit.protocolTerminal(ProtocolResult.failure(new IllegalStateException("stream closed")));
        assertThat(permit.releasedFuture()).isCompleted();
    }

    @Test
    @DisplayName("a synchronous Broker dispatch failure becomes an exceptional future and terminates the backend")
    public void synchronousDispatchFailureTerminatesBackend() {
        GrpcMessagingActivity delegate = mock(GrpcMessagingActivity.class);
        IllegalStateException failure = new IllegalStateException("dispatch failed");
        when(delegate.sendMessage(any(), any())).thenThrow(failure);
        SendDrainGate gate = new SendDrainGate();
        SendPermit permit = gate.tryAcquire(SendProtocol.GRPC).get();

        SendLifecycleMessagingActivity activity = new SendLifecycleMessagingActivity(delegate);
        CompletableFuture<SendMessageResponse> responseFuture =
            activity.sendMessage(ProxyContext.create().setSendLifecycleContext(permit), REQUEST);

        assertThat(responseFuture).isCompletedExceptionally();
        permit.protocolTerminal(ProtocolResult.failure(failure));
        assertThat(permit.releasedFuture()).isCompleted();
    }

    @Test
    @DisplayName("a null Broker future is treated as dispatch failure and terminates the backend")
    public void nullBrokerFutureTerminatesBackend() {
        GrpcMessagingActivity delegate = mock(GrpcMessagingActivity.class);
        when(delegate.sendMessage(any(), any())).thenReturn(null);
        SendDrainGate gate = new SendDrainGate();
        SendPermit permit = gate.tryAcquire(SendProtocol.GRPC).get();

        SendLifecycleMessagingActivity activity = new SendLifecycleMessagingActivity(delegate);
        CompletableFuture<SendMessageResponse> responseFuture =
            activity.sendMessage(ProxyContext.create().setSendLifecycleContext(permit), REQUEST);

        assertThat(responseFuture).isCompletedExceptionally();
        permit.protocolTerminal(ProtocolResult.failure(new NullPointerException("broker future")));
        assertThat(permit.releasedFuture()).isCompleted();
    }
}
