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
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SyncLiteSubscriptionRequest;
import apache.rocketmq.v2.SyncLiteSubscriptionResponse;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.consumer.LabelGroupBootstrapper;
import org.apache.rocketmq.proxy.grpc.v2.consumer.LabelRoutingResolver;
import org.apache.rocketmq.proxy.grpc.v2.consumer.TrafficLabel;
import org.apache.rocketmq.proxy.grpc.v2.consumer.TrafficLabelRouter;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TrafficLabelGrpcMessagingActivityTest extends InitConfigTest {

    private static final String CLIENT_ID = "client-id";
    private static final String TOPIC = "T";
    private static final String GROUP = "G";
    private static final String TRAFFIC_LABEL = "gray";
    private static final String EFFECTIVE_GROUP = "G%gray";

    private GrpcMessagingActivity delegate;
    private StubGrpcClientSettingsManager settingsManager;
    private SuccessfulLabelGroupBootstrapper bootstrapper;
    private TrafficLabelGrpcMessagingActivity activity;

    @Before
    public void setUp() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(false);
        delegate = mock(GrpcMessagingActivity.class);
        settingsManager = new StubGrpcClientSettingsManager();
        bootstrapper = new SuccessfulLabelGroupBootstrapper();
        TrafficLabelRouter router = new TrafficLabelRouter(
            new LabelRoutingResolver(), bootstrapper);
        activity = new TrafficLabelGrpcMessagingActivity(delegate, router, settingsManager);
    }

    @Test
    @DisplayName("Ack delegates an effective gray group without changing the client request")
    public void ackMessageRewritesGrayGroupBeforeDelegating() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        ProxyContext ctx = createGrayContext();
        AckMessageRequest request = AckMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName(TOPIC))
            .setGroup(Resource.newBuilder().setName(GROUP))
            .build();
        AckMessageResponse response = AckMessageResponse.getDefaultInstance();
        when(delegate.ackMessage(any(ProxyContext.class), any(AckMessageRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(response));

        assertThat(activity.ackMessage(ctx, request).join()).isSameAs(response);

        ArgumentCaptor<AckMessageRequest> requestCaptor = ArgumentCaptor.forClass(AckMessageRequest.class);
        verify(delegate).ackMessage(same(ctx), requestCaptor.capture());
        assertThat(requestCaptor.getValue().getGroup().getName()).isEqualTo(EFFECTIVE_GROUP);
        assertThat(request.getGroup().getName()).isEqualTo(GROUP);
    }

    @Test
    @DisplayName("SendMessage is transparently delegated")
    public void sendMessageIsDelegatedWithoutTransformation() {
        ProxyContext ctx = ProxyContext.create().setClientID(CLIENT_ID);
        SendMessageRequest request = SendMessageRequest.getDefaultInstance();
        SendMessageResponse response = SendMessageResponse.getDefaultInstance();
        when(delegate.sendMessage(same(ctx), same(request)))
            .thenReturn(CompletableFuture.completedFuture(response));

        assertThat(activity.sendMessage(ctx, request).join()).isSameAs(response);

        verify(delegate).sendMessage(same(ctx), same(request));
    }

    @Test
    @DisplayName("Ack keeps the logical group when traffic-label routing is disabled")
    public void ackMessageKeepsLogicalGroupWhenFeatureIsDisabled() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(false);
        ProxyContext ctx = createGrayContext();
        AckMessageRequest request = AckMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName(TOPIC))
            .setGroup(Resource.newBuilder().setName(GROUP))
            .build();
        when(delegate.ackMessage(any(ProxyContext.class), any(AckMessageRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(AckMessageResponse.getDefaultInstance()));

        activity.ackMessage(ctx, request).join();

        ArgumentCaptor<AckMessageRequest> requestCaptor = ArgumentCaptor.forClass(AckMessageRequest.class);
        verify(delegate).ackMessage(same(ctx), requestCaptor.capture());
        assertThat(requestCaptor.getValue()).isSameAs(request);
        assertThat(requestCaptor.getValue().getGroup().getName()).isEqualTo(GROUP);
    }

    @Test
    @DisplayName("Every group-bearing unary consumer request delegates the effective group")
    public void groupBearingUnaryRequestsDelegateEffectiveGroup() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        ProxyContext ctx = createGrayContext();
        Resource group = Resource.newBuilder().setName(GROUP).build();
        HeartbeatRequest heartbeatRequest = HeartbeatRequest.newBuilder().setGroup(group).build();
        QueryAssignmentRequest assignmentRequest = QueryAssignmentRequest.newBuilder().setGroup(group).build();
        ForwardMessageToDeadLetterQueueRequest forwardRequest =
            ForwardMessageToDeadLetterQueueRequest.newBuilder().setGroup(group).build();
        NotifyClientTerminationRequest terminationRequest =
            NotifyClientTerminationRequest.newBuilder().setGroup(group).build();
        ChangeInvisibleDurationRequest invisibleRequest =
            ChangeInvisibleDurationRequest.newBuilder().setGroup(group).build();
        SyncLiteSubscriptionRequest syncRequest =
            SyncLiteSubscriptionRequest.newBuilder().setGroup(group).build();
        when(delegate.heartbeat(any(), any())).thenReturn(
            CompletableFuture.completedFuture(HeartbeatResponse.getDefaultInstance()));
        when(delegate.queryAssignment(any(), any())).thenReturn(
            CompletableFuture.completedFuture(QueryAssignmentResponse.getDefaultInstance()));
        when(delegate.forwardMessageToDeadLetterQueue(any(), any())).thenReturn(
            CompletableFuture.completedFuture(ForwardMessageToDeadLetterQueueResponse.getDefaultInstance()));
        when(delegate.notifyClientTermination(any(), any())).thenReturn(
            CompletableFuture.completedFuture(NotifyClientTerminationResponse.getDefaultInstance()));
        when(delegate.changeInvisibleDuration(any(), any())).thenReturn(
            CompletableFuture.completedFuture(ChangeInvisibleDurationResponse.getDefaultInstance()));
        when(delegate.syncLiteSubscription(any(), any())).thenReturn(
            CompletableFuture.completedFuture(SyncLiteSubscriptionResponse.getDefaultInstance()));

        activity.heartbeat(ctx, heartbeatRequest).join();
        activity.queryAssignment(ctx, assignmentRequest).join();
        activity.forwardMessageToDeadLetterQueue(ctx, forwardRequest).join();
        activity.notifyClientTermination(ctx, terminationRequest).join();
        activity.changeInvisibleDuration(ctx, invisibleRequest).join();
        activity.syncLiteSubscription(ctx, syncRequest).join();

        ArgumentCaptor<HeartbeatRequest> heartbeatCaptor = ArgumentCaptor.forClass(HeartbeatRequest.class);
        ArgumentCaptor<QueryAssignmentRequest> assignmentCaptor =
            ArgumentCaptor.forClass(QueryAssignmentRequest.class);
        ArgumentCaptor<ForwardMessageToDeadLetterQueueRequest> forwardCaptor =
            ArgumentCaptor.forClass(ForwardMessageToDeadLetterQueueRequest.class);
        ArgumentCaptor<NotifyClientTerminationRequest> terminationCaptor =
            ArgumentCaptor.forClass(NotifyClientTerminationRequest.class);
        ArgumentCaptor<ChangeInvisibleDurationRequest> invisibleCaptor =
            ArgumentCaptor.forClass(ChangeInvisibleDurationRequest.class);
        ArgumentCaptor<SyncLiteSubscriptionRequest> syncCaptor =
            ArgumentCaptor.forClass(SyncLiteSubscriptionRequest.class);
        verify(delegate).heartbeat(same(ctx), heartbeatCaptor.capture());
        verify(delegate).queryAssignment(same(ctx), assignmentCaptor.capture());
        verify(delegate).forwardMessageToDeadLetterQueue(same(ctx), forwardCaptor.capture());
        verify(delegate).notifyClientTermination(same(ctx), terminationCaptor.capture());
        verify(delegate).changeInvisibleDuration(same(ctx), invisibleCaptor.capture());
        verify(delegate).syncLiteSubscription(same(ctx), syncCaptor.capture());
        assertThat(heartbeatCaptor.getValue().getGroup().getName()).isEqualTo(EFFECTIVE_GROUP);
        assertThat(assignmentCaptor.getValue().getGroup().getName()).isEqualTo(EFFECTIVE_GROUP);
        assertThat(forwardCaptor.getValue().getGroup().getName()).isEqualTo(EFFECTIVE_GROUP);
        assertThat(terminationCaptor.getValue().getGroup().getName()).isEqualTo(EFFECTIVE_GROUP);
        assertThat(invisibleCaptor.getValue().getGroup().getName()).isEqualTo(EFFECTIVE_GROUP);
        assertThat(syncCaptor.getValue().getGroup().getName()).isEqualTo(EFFECTIVE_GROUP);
        assertThat(heartbeatRequest.getGroup().getName()).isEqualTo(GROUP);
        assertThat(assignmentRequest.getGroup().getName()).isEqualTo(GROUP);
        assertThat(forwardRequest.getGroup().getName()).isEqualTo(GROUP);
        assertThat(terminationRequest.getGroup().getName()).isEqualTo(GROUP);
        assertThat(invisibleRequest.getGroup().getName()).isEqualTo(GROUP);
        assertThat(syncRequest.getGroup().getName()).isEqualTo(GROUP);
    }

    @Test
    @DisplayName("Receive delegates both the effective group and its SQL92 traffic-label filter")
    public void receiveMessageRewritesGroupAndFilterTogether() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        ProxyContext ctx = createGrayContext();
        ReceiveMessageRequest request = ReceiveMessageRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName(GROUP))
            .setMessageQueue(MessageQueue.newBuilder()
                .setTopic(Resource.newBuilder().setName(TOPIC)))
            .setFilterExpression(FilterExpression.newBuilder()
                .setType(FilterType.TAG)
                .setExpression("TagA"))
            .build();
        StreamObserver<ReceiveMessageResponse> responseObserver = mock(StreamObserver.class);

        activity.receiveMessage(ctx, request, responseObserver);

        ArgumentCaptor<ReceiveMessageRequest> requestCaptor =
            ArgumentCaptor.forClass(ReceiveMessageRequest.class);
        verify(delegate).receiveMessage(same(ctx), requestCaptor.capture(), same(responseObserver));
        ReceiveMessageRequest effectiveRequest = requestCaptor.getValue();
        assertThat(effectiveRequest.getGroup().getName()).isEqualTo(EFFECTIVE_GROUP);
        assertThat(effectiveRequest.getFilterExpression().getType()).isEqualTo(FilterType.SQL);
        assertThat(effectiveRequest.getFilterExpression().getExpression())
            .isEqualTo("( TAGS in ('TagA') ) AND ( __SERVICE_TAG__ = 'gray' )");
        assertThat(request.getGroup().getName()).isEqualTo(GROUP);
        assertThat(request.getFilterExpression().getType()).isEqualTo(FilterType.TAG);
        assertThat(request.getFilterExpression().getExpression()).isEqualTo("TagA");
    }

    @Test
    @DisplayName("Termination reuses the registered effective group when the later header changes")
    public void terminationPrefersRegisteredGroupOverChangedHeader() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        settingsManager.setSettings(consumerSettings(EFFECTIVE_GROUP));
        ProxyContext changedHeaderContext = ProxyContext.create()
            .setClientID(CLIENT_ID)
            .withVal(TrafficLabel.PROPERTY_KEY, "blue");
        NotifyClientTerminationRequest request = NotifyClientTerminationRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName(GROUP))
            .build();
        when(delegate.notifyClientTermination(any(), any())).thenReturn(
            CompletableFuture.completedFuture(NotifyClientTerminationResponse.getDefaultInstance()));

        activity.notifyClientTermination(changedHeaderContext, request).join();

        ArgumentCaptor<NotifyClientTerminationRequest> requestCaptor =
            ArgumentCaptor.forClass(NotifyClientTerminationRequest.class);
        verify(delegate).notifyClientTermination(same(changedHeaderContext), requestCaptor.capture());
        assertThat(requestCaptor.getValue().getGroup().getName()).isEqualTo(EFFECTIVE_GROUP);
    }

    @Test
    @DisplayName("A failed gray-group bootstrap stops registration before delegation")
    public void failedBootstrapStopsRegistrationBeforeDelegation() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        bootstrapper.ensureResult = false;
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName(GROUP))
            .build();

        assertThatThrownBy(() -> activity.heartbeat(createGrayContext(), request).join())
            .hasCauseInstanceOf(ProxyException.class)
            .hasMessageContaining(EFFECTIVE_GROUP);
        verify(delegate, never()).heartbeat(any(), any());
    }

    @Test
    @DisplayName("Telemetry delegates effective settings and restores the logical group for the client")
    public void telemetryRewritesInboundSettingsAndRestoresOutboundSettings() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        AtomicReference<TelemetryCommand> delegatedCommand = new AtomicReference<>();
        when(delegate.telemetry(any())).thenAnswer(invocation -> {
            StreamObserver<TelemetryCommand> delegateResponseObserver = invocation.getArgument(0);
            return new ContextStreamObserver<TelemetryCommand>() {
                @Override
                public void onNext(ProxyContext ctx, TelemetryCommand value) {
                    delegatedCommand.set(value);
                    settingsManager.setSettings(value.getSettings());
                    delegateResponseObserver.onNext(value);
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onCompleted() {
                    delegateResponseObserver.onCompleted();
                }
            };
        });
        AtomicReference<TelemetryCommand> clientCommand = new AtomicReference<>();
        StreamObserver<TelemetryCommand> clientResponseObserver = new StreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(TelemetryCommand value) {
                clientCommand.set(value);
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
            }
        };
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName(GROUP)))
            .build();
        TelemetryCommand request = TelemetryCommand.newBuilder().setSettings(settings).build();

        activity.telemetry(clientResponseObserver).onNext(createGrayContext(), request);

        assertThat(delegatedCommand.get().getSettings().getSubscription().getGroup().getName())
            .isEqualTo(EFFECTIVE_GROUP);
        assertThat(clientCommand.get().getSettings().getSubscription().getGroup().getName()).isEqualTo(GROUP);
        assertThat(request.getSettings().getSubscription().getGroup().getName()).isEqualTo(GROUP);
        assertThat(settingsManager.getRawClientSettings(CLIENT_ID)
            .getSubscription().getGroup().getName()).isEqualTo(EFFECTIVE_GROUP);
        assertThat(bootstrapper.originGroup).isEqualTo(GROUP);
        assertThat(bootstrapper.effectiveGroup).isEqualTo(EFFECTIVE_GROUP);
    }

    private Settings consumerSettings(String group) {
        return Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName(group)))
            .build();
    }

    private ProxyContext createGrayContext() {
        return ProxyContext.create()
            .setClientID(CLIENT_ID)
            .withVal(TrafficLabel.PROPERTY_KEY, TRAFFIC_LABEL);
    }

    private static class StubGrpcClientSettingsManager extends GrpcClientSettingsManager {
        private Settings settings;

        StubGrpcClientSettingsManager() {
            super(mock(MessagingProcessor.class));
        }

        @Override
        public Settings getRawClientSettings(String clientId) {
            return settings;
        }

        void setSettings(Settings settings) {
            this.settings = settings;
        }
    }

    private static class SuccessfulLabelGroupBootstrapper extends LabelGroupBootstrapper {

        private String originGroup;
        private String effectiveGroup;
        private boolean ensureResult = true;

        SuccessfulLabelGroupBootstrapper() {
            super(mock(AdminService.class));
        }

        @Override
        public boolean ensureGrayGroupFromOrigin(String originGroup, String effectiveGroup) {
            this.originGroup = originGroup;
            this.effectiveGroup = effectiveGroup;
            return ensureResult;
        }
    }
}
