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
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;

public class EffectiveConsumerGroupResolverTest extends InitConfigTest {

    private static final String CLIENT_ID = "client-id";
    private static final String LOGICAL_GROUP = "G";
    private static final String EFFECTIVE_GROUP = "G%gray";

    private BootstrapperSpy bootstrapper;
    private StubGrpcClientSettingsManager settingsManager;
    private EffectiveConsumerGroupResolver resolver;

    @Before
    public void setUp() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(false);
        bootstrapper = new BootstrapperSpy();
        settingsManager = new StubGrpcClientSettingsManager();
        TrafficLabelRouter router = new TrafficLabelRouter(new LabelRoutingResolver(), bootstrapper);
        resolver = new EffectiveConsumerGroupResolver(router, settingsManager);
    }

    @Test
    @DisplayName("Disabled routing always keeps the logical consumer group")
    public void disabledRoutingKeepsLogicalGroup() {
        settingsManager.setSettings(consumerSettings(EFFECTIVE_GROUP));
        ProxyContext ctx = grayContext();

        assertThat(resolver.resolveForRegistration(ctx, LOGICAL_GROUP)).isEqualTo(LOGICAL_GROUP);
        assertThat(resolver.resolveForRequest(ctx, LOGICAL_GROUP)).isEqualTo(LOGICAL_GROUP);
        assertThat(resolver.resolveForReceive(ctx, "T", LOGICAL_GROUP, null, ExpressionType.TAG)).isNull();
        assertThat(bootstrapper.calls).isEmpty();
    }

    @Test
    @DisplayName("Registration creates an effective group when the client has no binding")
    public void registrationRewritesAnUnboundGrayGroup() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);

        assertThat(resolver.resolveForRegistration(grayContext(), LOGICAL_GROUP)).isEqualTo(EFFECTIVE_GROUP);
        assertThat(bootstrapper.calls).containsExactly(LOGICAL_GROUP + "->" + EFFECTIVE_GROUP);
    }

    @Test
    @DisplayName("Registration reuses the cached effective group without provisioning it again")
    public void registrationReusesMatchingEffectiveGroup() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        settingsManager.setSettings(consumerSettings(EFFECTIVE_GROUP));

        assertThat(resolver.resolveForRegistration(standardContext(), LOGICAL_GROUP)).isEqualTo(EFFECTIVE_GROUP);
        assertThat(bootstrapper.calls).isEmpty();
    }

    @Test
    @DisplayName("Requests reuse the registered effective group when the traffic label header is absent")
    public void requestReusesEffectiveGroupWithoutHeader() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        settingsManager.setSettings(consumerSettings(EFFECTIVE_GROUP));

        assertThat(resolver.resolveForRequest(standardContext(), LOGICAL_GROUP)).isEqualTo(EFFECTIVE_GROUP);
    }

    @Test
    @DisplayName("A standard registration remains standard even if a later request carries a gray header")
    public void requestPrefersStandardRegistrationOverLaterHeader() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        settingsManager.setSettings(consumerSettings(LOGICAL_GROUP));

        assertThat(resolver.resolveForRequest(grayContext(), LOGICAL_GROUP)).isEqualTo(LOGICAL_GROUP);
    }

    @Test
    @DisplayName("A cached group for another logical consumer does not affect request routing")
    public void requestIgnoresAnUnrelatedCachedGroup() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        settingsManager.setSettings(consumerSettings("OTHER%blue"));

        assertThat(resolver.resolveForRequest(grayContext(), LOGICAL_GROUP)).isEqualTo(EFFECTIVE_GROUP);
    }

    @Test
    @DisplayName("Receive derives the gray filter from the cached effective group without a header")
    public void receiveUsesCachedEffectiveGroupWithoutHeader() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        settingsManager.setSettings(consumerSettings(EFFECTIVE_GROUP));

        LabelRoutingResolver.RoutingDecision decision = resolver.resolveForReceive(
            standardContext(), "T", LOGICAL_GROUP, "TagA", ExpressionType.TAG);

        assertThat(decision.getEffectiveGroup()).isEqualTo(EFFECTIVE_GROUP);
        assertThat(decision.getSql92())
            .isEqualTo("( TAGS in ('TagA') ) AND ( __SERVICE_TAG__ = 'gray' )");
    }

    private ProxyContext grayContext() {
        return standardContext().withVal(TrafficLabel.PROPERTY_KEY, "gray");
    }

    private ProxyContext standardContext() {
        return ProxyContext.create().setClientID(CLIENT_ID);
    }

    private Settings consumerSettings(String group) {
        return Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName(group)))
            .build();
    }

    private static class StubGrpcClientSettingsManager extends GrpcClientSettingsManager {
        private Settings settings;

        StubGrpcClientSettingsManager() {
            super(null);
        }

        @Override
        public Settings getRawClientSettings(String clientId) {
            return settings;
        }

        void setSettings(Settings settings) {
            this.settings = settings;
        }
    }

    private static class BootstrapperSpy extends LabelGroupBootstrapper {
        private final List<String> calls = new ArrayList<>();

        BootstrapperSpy() {
            super(null);
        }

        @Override
        public boolean ensureGrayGroupFromOrigin(String originGroup, String effectiveGroup) {
            calls.add(originGroup + "->" + effectiveGroup);
            return true;
        }
    }
}
