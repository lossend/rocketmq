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
package org.apache.rocketmq.proxy.grpc.v2.client;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.proxy.common.ContextVariable;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.consumer.LabelGroupBootstrapper;
import org.apache.rocketmq.proxy.grpc.v2.consumer.LabelRoutingResolver;
import org.apache.rocketmq.proxy.grpc.v2.consumer.StandardFilterAssembler;
import org.apache.rocketmq.proxy.grpc.v2.consumer.TopicClientInfoIndex;
import org.apache.rocketmq.proxy.grpc.v2.consumer.TrafficLabel;
import org.apache.rocketmq.proxy.grpc.v2.consumer.TrafficLabelRouter;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentCaptor.forClass;
import org.mockito.ArgumentCaptor;

/**
 * Verifies that {@link ClientActivity} rewrites the consumer group for gray consumers
 * at registration/termination time via {@link TrafficLabelRouter#rewriteRegistrationGroup}.
 *
 * <p>Uses hand-written stubs for classes that cannot be byte-buddy mocked on Java 21
 * ({@link GrpcClientSettingsManager} extends {@code ServiceThread}).
 */
public class ClientActivityTrafficLabelTest {

    private static final String CONSUMER_GROUP = "G";
    private static final String GRAY_LABEL = "gray1";
    private static final String GRAY_GROUP = "G%gray1";
    private static final String CLIENT_ID = "test-client-" + UUID.randomUUID();

    /** Hand-written stub — GrpcClientSettingsManager extends ServiceThread (a Thread subclass)
     *  which byte-buddy/JaCoCo cannot instrument on Java 21. */
    private static class SettingsManagerStub extends GrpcClientSettingsManager {
        private Settings returnOnRemove;

        SettingsManagerStub(MessagingProcessor proc) {
            super(proc);
        }

        @Override
        public Settings removeAndGetClientSettings(ProxyContext ctx) {
            return returnOnRemove;
        }

        @Override
        public String getServiceName() {
            return "SettingsManagerStub";
        }
    }

    /** Hand-written bootstrapper stub — avoids AdminService interactions. */
    private static class NoopBootstrapper extends LabelGroupBootstrapper {
        NoopBootstrapper() {
            super(mock(AdminService.class));
        }

        @Override
        public void ensureGroup(String topic, String group) {
        }
    }

    private MessagingProcessor messagingProcessor;
    private SettingsManagerStub settingsManager;
    private ClientActivity clientActivity;

    @BeforeClass
    public static void initConfig() throws Exception {
        ConfigurationManager.initEnv();
        ConfigurationManager.initConfig();
    }

    @Before
    public void setUp() throws Exception {
        messagingProcessor = mock(MessagingProcessor.class);
        ProxyRelayService relayService = mock(ProxyRelayService.class);
        doNothing().when(messagingProcessor).registerConsumer(
            any(), any(), any(), any(), any(), any(), any(), anyBoolean());
        doNothing().when(messagingProcessor).unRegisterConsumer(any(), any(), any());

        settingsManager = new SettingsManagerStub(messagingProcessor);
        GrpcChannelManager channelManager = new GrpcChannelManager(relayService, settingsManager);

        clientActivity = new ClientActivity(messagingProcessor, settingsManager, channelManager);

        TrafficLabelRouter router = new TrafficLabelRouter(
            new LabelRoutingResolver(), new NoopBootstrapper(),
            new TopicClientInfoIndex(), new StandardFilterAssembler());
        clientActivity.setTrafficLabelRouter(router);

        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(false);
    }

    @Test
    public void registerConsumer_switch_on_gray_header_rewrites_group() throws Exception {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);

        clientActivity.registerConsumer(grayCtx(), CONSUMER_GROUP, ClientType.SIMPLE_CONSUMER,
            Collections.emptyList(), true);

        ArgumentCaptor<String> groupCaptor = forClass(String.class);
        verify(messagingProcessor).registerConsumer(
            any(), groupCaptor.capture(), any(), any(), any(), any(), any(), anyBoolean());
        assertThat(groupCaptor.getValue()).isEqualTo(GRAY_GROUP);
    }

    @Test
    public void registerConsumer_switch_on_standard_keeps_origin_group() throws Exception {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);

        clientActivity.registerConsumer(standardCtx(), CONSUMER_GROUP, ClientType.SIMPLE_CONSUMER,
            Collections.emptyList(), true);

        ArgumentCaptor<String> groupCaptor = forClass(String.class);
        verify(messagingProcessor).registerConsumer(
            any(), groupCaptor.capture(), any(), any(), any(), any(), any(), anyBoolean());
        assertThat(groupCaptor.getValue()).isEqualTo(CONSUMER_GROUP);
    }

    @Test
    public void registerConsumer_switch_off_keeps_origin_group_regardless_of_header() throws Exception {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(false);

        clientActivity.registerConsumer(grayCtx(), CONSUMER_GROUP, ClientType.SIMPLE_CONSUMER,
            Collections.emptyList(), true);

        ArgumentCaptor<String> groupCaptor = forClass(String.class);
        verify(messagingProcessor).registerConsumer(
            any(), groupCaptor.capture(), any(), any(), any(), any(), any(), anyBoolean());
        assertThat(groupCaptor.getValue()).isEqualTo(CONSUMER_GROUP);
    }

    @Test
    public void notifyClientTermination_switch_on_gray_header_rewrites_group() throws Exception {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        // Register first so the channel exists for removeChannel to return non-null.
        clientActivity.registerConsumer(grayCtx(), CONSUMER_GROUP, ClientType.SIMPLE_CONSUMER,
            Collections.emptyList(), true);

        settingsManager.returnOnRemove = Settings.newBuilder()
            .setClientType(ClientType.SIMPLE_CONSUMER)
            .build();

        clientActivity.notifyClientTermination(grayCtx(),
            NotifyClientTerminationRequest.newBuilder()
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .build()).get();

        ArgumentCaptor<String> groupCaptor = forClass(String.class);
        // First call is from registerConsumer; second from unRegisterConsumer.
        verify(messagingProcessor).unRegisterConsumer(any(), groupCaptor.capture(),
            any(ClientChannelInfo.class));
        assertThat(groupCaptor.getValue()).isEqualTo(GRAY_GROUP);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private ProxyContext grayCtx() {
        return baseCtx().withVal(TrafficLabel.PROPERTY_KEY, GRAY_LABEL);
    }

    private ProxyContext standardCtx() {
        return baseCtx();
    }

    private ProxyContext baseCtx() {
        return ProxyContext.create()
            .withVal(ContextVariable.CLIENT_ID, CLIENT_ID)
            .withVal(ContextVariable.LANGUAGE, "JAVA")
            .withVal(ContextVariable.REMOTE_ADDRESS, "192.168.0.1:8080")
            .withVal(ContextVariable.LOCAL_ADDRESS, "127.0.0.1:8080")
            .withVal(ContextVariable.REMAINING_MS, Duration.ofSeconds(10).toMillis());
    }
}
