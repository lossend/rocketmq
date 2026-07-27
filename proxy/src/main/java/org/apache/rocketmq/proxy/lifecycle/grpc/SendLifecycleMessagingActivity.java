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

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.RecallMessageRequest;
import apache.rocketmq.v2.RecallMessageResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.SyncLiteSubscriptionRequest;
import apache.rocketmq.v2.SyncLiteSubscriptionResponse;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.ContextStreamObserver;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingActivity;
import org.apache.rocketmq.proxy.lifecycle.SendLifecycleContext;

/**
 * Decorates a {@link GrpcMessagingActivity} to bind each SendMessage to its
 * send-drain permit without touching the business method. The permit is placed
 * on the {@link ProxyContext} upstream (on the service thread); here, on the
 * worker thread, the backend terminal is driven around the delegate call. Every
 * other RPC is a straight pass-through, so the decorator is inert unless the
 * graceful lifecycle is enabled.
 */
public final class SendLifecycleMessagingActivity implements GrpcMessagingActivity {

    private final GrpcMessagingActivity delegate;

    public SendLifecycleMessagingActivity(GrpcMessagingActivity delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletableFuture<SendMessageResponse> sendMessage(ProxyContext ctx, SendMessageRequest request) {
        SendLifecycleContext lifecycle = ctx.getSendLifecycleContext();
        if (lifecycle == null) {
            return delegate.sendMessage(ctx, request);
        }
        if (!lifecycle.backendStarted()) {
            // Cancelled/skipped before dispatch: never call the Broker. The permit
            // still releases via the tracer's protocol terminal, so leave the future
            // uncompleted (no response is written to an already-closed stream).
            return new CompletableFuture<>();
        }
        CompletableFuture<SendMessageResponse> future = delegate.sendMessage(ctx, request);
        future.whenComplete((response, throwable) -> lifecycle.backendTerminal(throwable));
        return future;
    }

    @Override
    public void start() throws Exception {
        delegate.start();
    }

    @Override
    public void shutdown() throws Exception {
        delegate.shutdown();
    }

    @Override
    public CompletableFuture<QueryRouteResponse> queryRoute(ProxyContext ctx, QueryRouteRequest request) {
        return delegate.queryRoute(ctx, request);
    }

    @Override
    public CompletableFuture<HeartbeatResponse> heartbeat(ProxyContext ctx, HeartbeatRequest request) {
        return delegate.heartbeat(ctx, request);
    }

    @Override
    public CompletableFuture<QueryAssignmentResponse> queryAssignment(ProxyContext ctx,
        QueryAssignmentRequest request) {
        return delegate.queryAssignment(ctx, request);
    }

    @Override
    public void receiveMessage(ProxyContext ctx, ReceiveMessageRequest request,
        StreamObserver<ReceiveMessageResponse> responseObserver) {
        delegate.receiveMessage(ctx, request, responseObserver);
    }

    @Override
    public CompletableFuture<AckMessageResponse> ackMessage(ProxyContext ctx, AckMessageRequest request) {
        return delegate.ackMessage(ctx, request);
    }

    @Override
    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(
        ProxyContext ctx, ForwardMessageToDeadLetterQueueRequest request) {
        return delegate.forwardMessageToDeadLetterQueue(ctx, request);
    }

    @Override
    public CompletableFuture<EndTransactionResponse> endTransaction(ProxyContext ctx,
        EndTransactionRequest request) {
        return delegate.endTransaction(ctx, request);
    }

    @Override
    public CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(ProxyContext ctx,
        NotifyClientTerminationRequest request) {
        return delegate.notifyClientTermination(ctx, request);
    }

    @Override
    public CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(ProxyContext ctx,
        ChangeInvisibleDurationRequest request) {
        return delegate.changeInvisibleDuration(ctx, request);
    }

    @Override
    public CompletableFuture<RecallMessageResponse> recallMessage(ProxyContext ctx, RecallMessageRequest request) {
        return delegate.recallMessage(ctx, request);
    }

    @Override
    public CompletableFuture<SyncLiteSubscriptionResponse> syncLiteSubscription(ProxyContext ctx,
        SyncLiteSubscriptionRequest request) {
        return delegate.syncLiteSubscription(ctx, request);
    }

    @Override
    public ContextStreamObserver<TelemetryCommand> telemetry(StreamObserver<TelemetryCommand> responseObserver) {
        return delegate.telemetry(responseObserver);
    }
}
