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
package org.apache.rocketmq.proxy.grpc.v2.common;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GrpcClientSettingsManagerCleanerTest extends InitConfigTest {

    private static final String CLIENT_ID = "client-id";
    private static final String LOGICAL_GROUP = "G";
    private static final String EFFECTIVE_GROUP = "G%gray";

    private MessagingProcessor messagingProcessor;
    private GrpcClientSettingsManager settingsManager;

    @Before
    public void setUp() {
        GrpcClientSettingsManager.CLIENT_SETTINGS_MAP.clear();
        messagingProcessor = mock(MessagingProcessor.class);
        settingsManager = new GrpcClientSettingsManager(messagingProcessor);
    }

    @After
    public void tearDown() {
        GrpcClientSettingsManager.CLIENT_SETTINGS_MAP.clear();
    }

    @Test
    @DisplayName("Cleaner checks the cached effective group and retains its active client")
    public void cleanerRetainsClientRegisteredOnEffectiveGroup() {
        ProxyContext ctx = ProxyContext.create().setClientID(CLIENT_ID);
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName(EFFECTIVE_GROUP)))
            .build();
        settingsManager.updateClientSettings(ctx, CLIENT_ID, settings);
        ConsumerGroupInfo consumerGroupInfo = mock(ConsumerGroupInfo.class);
        when(messagingProcessor.getConsumerGroupInfo(any(), eq(EFFECTIVE_GROUP)))
            .thenReturn(consumerGroupInfo);
        when(consumerGroupInfo.findChannel(CLIENT_ID)).thenReturn(mock(ClientChannelInfo.class));

        settingsManager.onWaitEnd();

        assertThat(settingsManager.getRawClientSettings(CLIENT_ID)).isNotNull();
        verify(messagingProcessor).getConsumerGroupInfo(any(), eq(EFFECTIVE_GROUP));
        verify(messagingProcessor, never()).getConsumerGroupInfo(any(), eq(LOGICAL_GROUP));
    }
}
