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
package org.apache.rocketmq.proxy.grpc.v2;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.FilterType;
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
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.SyncLiteSubscriptionRequest;
import apache.rocketmq.v2.SyncLiteSubscriptionResponse;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcValidator;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseWriter;
import org.apache.rocketmq.proxy.grpc.v2.consumer.EffectiveConsumerGroupResolver;
import org.apache.rocketmq.proxy.grpc.v2.consumer.LabelGroupBootstrapper;
import org.apache.rocketmq.proxy.grpc.v2.consumer.LabelRoutingResolver;
import org.apache.rocketmq.proxy.grpc.v2.consumer.StandardFilterAssembler;
import org.apache.rocketmq.proxy.grpc.v2.consumer.TopicClientInfoIndex;
import org.apache.rocketmq.proxy.grpc.v2.consumer.TrafficLabelRouter;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;

/**
 * Converts client-visible logical consumer groups to the effective groups used by Proxy internals.
 * Authentication and authorization run before this decorator and therefore continue to observe the
 * original client request.
 */
public class TrafficLabelGrpcMessagingActivity implements GrpcMessagingActivity {

    private final GrpcMessagingActivity delegate;
    private final EffectiveConsumerGroupResolver groupResolver;

    public TrafficLabelGrpcMessagingActivity(GrpcMessagingActivity delegate, TrafficLabelRouter trafficLabelRouter,
        GrpcClientSettingsManager grpcClientSettingsManager) {
        this.delegate = delegate;
        this.groupResolver = new EffectiveConsumerGroupResolver(trafficLabelRouter, grpcClientSettingsManager);
    }

    public static TrafficLabelGrpcMessagingActivity create(GrpcMessagingActivity delegate,
        MessagingProcessor messagingProcessor, GrpcClientSettingsManager grpcClientSettingsManager) {
        MetadataService metadataService = messagingProcessor.getMetadataService();
        LabelGroupBootstrapper bootstrapper = new LabelGroupBootstrapper(
            messagingProcessor.getAdminService(), metadataService);
        TopicClientInfoIndex topicClientInfoIndex = new TopicClientInfoIndex();
        messagingProcessor.registerConsumerListener(topicClientInfoIndex);
        TrafficLabelRouter trafficLabelRouter = new TrafficLabelRouter(
            new LabelRoutingResolver(), bootstrapper, topicClientInfoIndex,
            new StandardFilterAssembler(), metadataService);
        return new TrafficLabelGrpcMessagingActivity(delegate, trafficLabelRouter, grpcClientSettingsManager);
    }

    @Override
    public CompletableFuture<QueryRouteResponse> queryRoute(ProxyContext ctx, QueryRouteRequest request) {
        return delegate.queryRoute(ctx, request);
    }

    @Override
    public CompletableFuture<HeartbeatResponse> heartbeat(ProxyContext ctx, HeartbeatRequest request) {
        try {
            if (!request.hasGroup()) {
                return delegate.heartbeat(ctx, request);
            }
            String effectiveGroup = resolveRegistrationGroup(ctx, request.getGroup());
            HeartbeatRequest effectiveRequest = request;
            if (!effectiveGroup.equals(request.getGroup().getName())) {
                effectiveRequest = request.toBuilder()
                    .setGroup(withName(request.getGroup(), effectiveGroup))
                    .build();
            }
            return delegate.heartbeat(ctx, effectiveRequest);
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    @Override
    public CompletableFuture<SendMessageResponse> sendMessage(ProxyContext ctx, SendMessageRequest request) {
        return delegate.sendMessage(ctx, request);
    }

    @Override
    public CompletableFuture<QueryAssignmentResponse> queryAssignment(ProxyContext ctx,
        QueryAssignmentRequest request) {
        try {
            String effectiveGroup = resolveRequestGroup(ctx, request.getGroup());
            QueryAssignmentRequest effectiveRequest = request;
            if (!effectiveGroup.equals(request.getGroup().getName())) {
                effectiveRequest = request.toBuilder()
                    .setGroup(withName(request.getGroup(), effectiveGroup))
                    .build();
            }
            return delegate.queryAssignment(ctx, effectiveRequest);
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    @Override
    public void receiveMessage(ProxyContext ctx, ReceiveMessageRequest request,
        StreamObserver<ReceiveMessageResponse> responseObserver) {
        try {
            GrpcValidator.getInstance().validateTopicAndConsumerGroup(
                request.getMessageQueue().getTopic(), request.getGroup());
            FilterExpression filterExpression = request.getFilterExpression();
            LabelRoutingResolver.RoutingDecision decision = groupResolver.resolveForReceive(
                ctx,
                request.getMessageQueue().getTopic().getName(),
                request.getGroup().getName(),
                filterExpression.getExpression(),
                GrpcConverter.getInstance().buildExpressionType(filterExpression.getType())
            );
            ReceiveMessageRequest effectiveRequest = request;
            if (decision != null) {
                effectiveRequest = request.toBuilder()
                    .setGroup(withName(request.getGroup(), decision.getEffectiveGroup()))
                    .setFilterExpression(FilterExpression.newBuilder()
                        .setType(FilterType.SQL)
                        .setExpression(decision.getSql92()))
                    .build();
            }
            delegate.receiveMessage(ctx, effectiveRequest, responseObserver);
        } catch (Throwable t) {
            ReceiveMessageResponse response = ReceiveMessageResponse.newBuilder()
                .setStatus(ResponseBuilder.getInstance().buildStatus(t))
                .build();
            ResponseWriter.getInstance().write(responseObserver, response);
        }
    }

    @Override
    public CompletableFuture<AckMessageResponse> ackMessage(ProxyContext ctx, AckMessageRequest request) {
        try {
            String effectiveGroup = resolveRequestGroup(ctx, request.getGroup());
            AckMessageRequest effectiveRequest = request;
            if (!effectiveGroup.equals(request.getGroup().getName())) {
                effectiveRequest = request.toBuilder()
                    .setGroup(withName(request.getGroup(), effectiveGroup))
                    .build();
            }
            return delegate.ackMessage(ctx, effectiveRequest);
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    @Override
    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(
        ProxyContext ctx, ForwardMessageToDeadLetterQueueRequest request) {
        try {
            String effectiveGroup = resolveRequestGroup(ctx, request.getGroup());
            ForwardMessageToDeadLetterQueueRequest effectiveRequest = request;
            if (!effectiveGroup.equals(request.getGroup().getName())) {
                effectiveRequest = request.toBuilder()
                    .setGroup(withName(request.getGroup(), effectiveGroup))
                    .build();
            }
            return delegate.forwardMessageToDeadLetterQueue(ctx, effectiveRequest);
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    @Override
    public CompletableFuture<EndTransactionResponse> endTransaction(ProxyContext ctx,
        EndTransactionRequest request) {
        return delegate.endTransaction(ctx, request);
    }

    @Override
    public CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(ProxyContext ctx,
        NotifyClientTerminationRequest request) {
        try {
            if (!request.hasGroup()) {
                return delegate.notifyClientTermination(ctx, request);
            }
            String effectiveGroup = resolveRequestGroup(ctx, request.getGroup());
            NotifyClientTerminationRequest effectiveRequest = request;
            if (!effectiveGroup.equals(request.getGroup().getName())) {
                effectiveRequest = request.toBuilder()
                    .setGroup(withName(request.getGroup(), effectiveGroup))
                    .build();
            }
            return delegate.notifyClientTermination(ctx, effectiveRequest);
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    @Override
    public CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(ProxyContext ctx,
        ChangeInvisibleDurationRequest request) {
        try {
            String effectiveGroup = resolveRequestGroup(ctx, request.getGroup());
            ChangeInvisibleDurationRequest effectiveRequest = request;
            if (!effectiveGroup.equals(request.getGroup().getName())) {
                effectiveRequest = request.toBuilder()
                    .setGroup(withName(request.getGroup(), effectiveGroup))
                    .build();
            }
            return delegate.changeInvisibleDuration(ctx, effectiveRequest);
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    @Override
    public CompletableFuture<RecallMessageResponse> recallMessage(ProxyContext ctx, RecallMessageRequest request) {
        return delegate.recallMessage(ctx, request);
    }

    @Override
    public CompletableFuture<SyncLiteSubscriptionResponse> syncLiteSubscription(ProxyContext ctx,
        SyncLiteSubscriptionRequest request) {
        try {
            String effectiveGroup = resolveRequestGroup(ctx, request.getGroup());
            SyncLiteSubscriptionRequest effectiveRequest = request;
            if (!effectiveGroup.equals(request.getGroup().getName())) {
                effectiveRequest = request.toBuilder()
                    .setGroup(withName(request.getGroup(), effectiveGroup))
                    .build();
            }
            return delegate.syncLiteSubscription(ctx, effectiveRequest);
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    @Override
    public ContextStreamObserver<TelemetryCommand> telemetry(StreamObserver<TelemetryCommand> responseObserver) {
        Map<String, Resource> logicalGroupByEffectiveGroup = new ConcurrentHashMap<>();
        StreamObserver<TelemetryCommand> restoringObserver = restoringTelemetryObserver(
            responseObserver, logicalGroupByEffectiveGroup);
        ContextStreamObserver<TelemetryCommand> delegateObserver = delegate.telemetry(restoringObserver);
        return new ContextStreamObserver<TelemetryCommand>() {
            private boolean terminated;

            @Override
            public synchronized void onNext(ProxyContext ctx, TelemetryCommand value) {
                if (terminated) {
                    return;
                }
                try {
                    delegateObserver.onNext(ctx,
                        rewriteTelemetrySettings(ctx, value, logicalGroupByEffectiveGroup));
                } catch (Throwable t) {
                    terminated = true;
                    logicalGroupByEffectiveGroup.clear();
                    responseObserver.onError(toTelemetryException(t));
                }
            }

            @Override
            public synchronized void onError(Throwable t) {
                if (terminated) {
                    return;
                }
                terminated = true;
                logicalGroupByEffectiveGroup.clear();
                delegateObserver.onError(t);
            }

            @Override
            public synchronized void onCompleted() {
                if (terminated) {
                    return;
                }
                terminated = true;
                try {
                    delegateObserver.onCompleted();
                } finally {
                    logicalGroupByEffectiveGroup.clear();
                }
            }
        };
    }

    protected TelemetryCommand rewriteTelemetrySettings(ProxyContext ctx, TelemetryCommand command,
        Map<String, Resource> logicalGroupByEffectiveGroup) {
        if (!command.hasSettings() || !command.getSettings().hasSubscription()) {
            return command;
        }
        Settings settings = command.getSettings();
        Resource logicalGroup = settings.getSubscription().getGroup();
        String effectiveGroup = resolveRegistrationGroup(ctx, logicalGroup);
        logicalGroupByEffectiveGroup.put(effectiveGroup, logicalGroup);
        if (effectiveGroup.equals(logicalGroup.getName())) {
            return command;
        }
        Settings effectiveSettings = settings.toBuilder()
            .setSubscription(settings.getSubscription().toBuilder()
                .setGroup(withName(logicalGroup, effectiveGroup)))
            .build();
        return command.toBuilder().setSettings(effectiveSettings).build();
    }

    protected StreamObserver<TelemetryCommand> restoringTelemetryObserver(
        StreamObserver<TelemetryCommand> responseObserver,
        Map<String, Resource> logicalGroupByEffectiveGroup) {
        return new StreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(TelemetryCommand value) {
                responseObserver.onNext(restoreLogicalGroup(value, logicalGroupByEffectiveGroup));
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    protected TelemetryCommand restoreLogicalGroup(TelemetryCommand command,
        Map<String, Resource> logicalGroupByEffectiveGroup) {
        if (!command.hasSettings() || !command.getSettings().hasSubscription()) {
            return command;
        }
        Settings settings = command.getSettings();
        String effectiveGroup = settings.getSubscription().getGroup().getName();
        Resource logicalGroup = logicalGroupByEffectiveGroup.get(effectiveGroup);
        if (logicalGroup == null || logicalGroup.equals(settings.getSubscription().getGroup())) {
            return command;
        }
        Settings logicalSettings = settings.toBuilder()
            .setSubscription(settings.getSubscription().toBuilder().setGroup(logicalGroup))
            .build();
        return command.toBuilder().setSettings(logicalSettings).build();
    }

    protected String resolveRegistrationGroup(ProxyContext ctx, Resource logicalGroup) {
        validateConsumerGroup(logicalGroup);
        return groupResolver.resolveForRegistration(ctx, logicalGroup.getName());
    }

    protected String resolveRequestGroup(ProxyContext ctx, Resource logicalGroup) {
        validateConsumerGroup(logicalGroup);
        return groupResolver.resolveForRequest(ctx, logicalGroup.getName());
    }

    protected void validateConsumerGroup(Resource logicalGroup) {
        GrpcValidator.getInstance().validateConsumerGroup(logicalGroup);
    }

    protected Resource withName(Resource resource, String name) {
        return resource.toBuilder().setName(name).build();
    }

    protected StatusRuntimeException toTelemetryException(Throwable t) {
        io.grpc.Status status = io.grpc.Status.INTERNAL;
        if (t instanceof GrpcProxyException) {
            Code code = ((GrpcProxyException) t).getCode();
            if (code.getNumber() < Code.INTERNAL_ERROR_VALUE && code.getNumber() >= Code.BAD_REQUEST_VALUE) {
                status = io.grpc.Status.INVALID_ARGUMENT;
            }
        }
        return status.withDescription("process client telemetryCommand failed. " + t.getMessage())
            .withCause(t)
            .asRuntimeException();
    }

    protected static <T> CompletableFuture<T> failedFuture(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
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
}
