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

import java.lang.reflect.Field;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.lifecycle.grpc.SendLifecycleMessagingActivity;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@code GrpcMessagingApplication.create} installs the SendPermit
 * accounting decorator only when {@code enableProxySendDrain} is set, so the
 * accounting can be switched off independently of the rest of the lifecycle.
 */
public class GrpcMessagingApplicationSendDrainToggleTest extends InitConfigTest {

    private MessagingProcessor messagingProcessor;

    @Before
    public void setUp() throws Throwable {
        super.before();
        messagingProcessor = Mockito.mock(MessagingProcessor.class);
    }

    @After
    public void reset() {
        ConfigurationManager.getProxyConfig().setEnableProxySendDrain(false);
    }

    private GrpcMessagingActivity activityOf(GrpcMessagingApplication application) throws Exception {
        Field field = GrpcMessagingApplication.class.getDeclaredField("grpcMessagingActivity");
        field.setAccessible(true);
        return (GrpcMessagingActivity) field.get(application);
    }

    @Test
    @DisplayName("send-drain off: create wires the plain activity without the lifecycle decorator")
    public void sendDrainOffSkipsDecorator() throws Exception {
        ConfigurationManager.getProxyConfig().setEnableProxySendDrain(false);
        GrpcMessagingApplication application = GrpcMessagingApplication.create(messagingProcessor);
        assertThat(activityOf(application)).isInstanceOf(DefaultGrpcMessagingActivity.class);
        assertThat(activityOf(application)).isNotInstanceOf(SendLifecycleMessagingActivity.class);
    }

    @Test
    @DisplayName("send-drain on: create wraps the activity in the lifecycle decorator")
    public void sendDrainOnInstallsDecorator() throws Exception {
        ConfigurationManager.getProxyConfig().setEnableProxySendDrain(true);
        GrpcMessagingApplication application = GrpcMessagingApplication.create(messagingProcessor);
        assertThat(activityOf(application)).isInstanceOf(SendLifecycleMessagingActivity.class);
    }
}
