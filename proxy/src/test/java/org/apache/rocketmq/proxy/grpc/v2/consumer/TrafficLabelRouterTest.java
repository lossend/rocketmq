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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link TrafficLabelRouter}.
 *
 * <p>Because the project uses Mockito 3.x (incompatible with Java 21 byte-buddy inline mocking
 * of concrete classes), {@link LabelGroupBootstrapper} is replaced with a hand-written spy
 * that records {@code ensureGroup} calls.
 */
public class TrafficLabelRouterTest {

    /**
     * Hand-written spy for {@link LabelGroupBootstrapper}
     * that records every (topic, group) pair passed to {@code ensureGroup}.
     */
    private static class BootstrapperSpy extends LabelGroupBootstrapper {

        final List<String[]> calls = new ArrayList<>();

        BootstrapperSpy() {
            super(mock(AdminService.class));
        }

        @Override
        public void ensureGroup(String topic, String effectiveGroup) {
            calls.add(new String[] {topic, effectiveGroup});
        }

        boolean neverCalled() {
            return calls.isEmpty();
        }

        boolean calledOnceWith(String topic, String group) {
            return calls.size() == 1
                && calls.get(0)[0].equals(topic)
                && calls.get(0)[1].equals(group);
        }
    }

    private BootstrapperSpy bootstrapper;
    private TopicClientInfoIndex index;
    private StandardFilterAssembler assembler;
    private TrafficLabelRouter router;

    @BeforeClass
    public static void initConfig() throws Exception {
        ConfigurationManager.initEnv();
        ConfigurationManager.initConfig();
    }

    @Before
    public void setUp() {
        bootstrapper = new BootstrapperSpy();
        index = new TopicClientInfoIndex();
        assembler = new StandardFilterAssembler();
        router = new TrafficLabelRouter(new LabelRoutingResolver(), bootstrapper, index, assembler);
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(false);
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRoutingLog(false);
    }

    @Test
    public void disabled_master_switch_returns_origin_group_untouched() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(false);
        ProxyContext ctx = ProxyContext.create();
        ctx.withVal(TrafficLabel.PROPERTY_KEY, "gray1");

        assertThat(router.rewriteGroup(ctx, "test-topic", "G")).isEqualTo("G");
        assertThat(bootstrapper.neverCalled()).isTrue();
    }

    @Test
    public void enabled_gray_rewrites_group_and_creates_group() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        ProxyContext ctx = ProxyContext.create();
        ctx.withVal(TrafficLabel.PROPERTY_KEY, "gray1");

        assertThat(router.rewriteGroup(ctx, "test-topic", "G")).isEqualTo("G%gray1");
        assertThat(bootstrapper.calledOnceWith("test-topic", "G%gray1")).isTrue();
    }

    @Test
    public void enabled_standard_keeps_origin_group_no_create() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        ProxyContext ctx = ProxyContext.create();

        assertThat(router.rewriteGroup(ctx, "test-topic", "G")).isEqualTo("G");
        assertThat(bootstrapper.neverCalled()).isTrue();
    }

    @Test
    public void resolve_for_receive_returns_decision_when_enabled() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        ProxyContext ctx = ProxyContext.create();
        ctx.withVal(TrafficLabel.PROPERTY_KEY, "gray1");

        LabelRoutingResolver.RoutingDecision d =
            router.resolveForReceive(ctx, "test-topic", "G", null, ExpressionType.TAG);
        assertThat(d.getEffectiveGroup()).isEqualTo("G%gray1");
        assertThat(d.getSql92()).isEqualTo("__SERVICE_TAG__ = 'gray1'");
    }

    @Test
    public void resolve_for_receive_returns_null_when_disabled() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(false);
        ProxyContext ctx = ProxyContext.create();
        ctx.withVal(TrafficLabel.PROPERTY_KEY, "gray1");

        assertThat(router.resolveForReceive(ctx, "test-topic", "G", null, ExpressionType.TAG)).isNull();
    }

    @Test
    public void resolve_for_receive_threads_tag_expression_type_through_to_sql92() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        ProxyContext ctx = ProxyContext.create();
        ctx.withVal(TrafficLabel.PROPERTY_KEY, "gray1");

        // TAG expression "TagA" must arrive at the broker as TAGS in ('TagA'), not raw "TagA".
        LabelRoutingResolver.RoutingDecision d =
            router.resolveForReceive(ctx, "test-topic", "G", "TagA", ExpressionType.TAG);
        assertThat(d.getSql92())
            .isEqualTo("( TAGS in ('TagA') ) AND ( __SERVICE_TAG__ = 'gray1' )");
    }

    // -------------------------------------------------------------------------
    // Task 4: dynamic standard-side filter + rewriteRegistrationGroup
    // -------------------------------------------------------------------------

    @Test
    public void disabled_switch_resolveForReceive_returns_null_and_rewriteRegistrationGroup_returns_input() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(false);
        ProxyContext ctx = ProxyContext.create();
        ctx.withVal(TrafficLabel.PROPERTY_KEY, "gray1");

        assertThat(router.resolveForReceive(ctx, "T", "G", null, null)).isNull();
        assertThat(router.rewriteRegistrationGroup(ctx, "G")).isEqualTo("G");
    }

    @Test
    public void standard_receive_with_active_gray_label_in_index_returns_exclusion_decision() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        // Simulate gray1 registering on topic T under logical group G.
        index.handle(ConsumerGroupEvent.CLIENT_REGISTER, "G%gray1",
            new Object[] {null, Collections.singleton("T")});

        ProxyContext ctx = ProxyContext.create(); // no label = standard consumer
        LabelRoutingResolver.RoutingDecision d =
            router.resolveForReceive(ctx, "T", "G", null, null);

        assertThat(d).isNotNull();
        assertThat(d.getEffectiveGroup()).isEqualTo("G");
        assertThat(d.getSql92()).isEqualTo(
            "__SERVICE_TAG__ IS NULL OR (__SERVICE_TAG__ <> 'gray1')");
        assertThat(bootstrapper.neverCalled()).isTrue();
    }

    @Test
    public void standard_receive_with_empty_index_returns_null() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        ProxyContext ctx = ProxyContext.create(); // no label = standard consumer

        assertThat(router.resolveForReceive(ctx, "T", "G", null, null)).isNull();
    }

    @Test
    public void gray_receive_with_active_index_is_unchanged_and_bootstraps() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        // Even with another gray label in the index, gray consumer uses its own resolver path.
        index.handle(ConsumerGroupEvent.CLIENT_REGISTER, "G%gray2",
            new Object[] {null, Collections.singleton("T")});

        ProxyContext ctx = ProxyContext.create();
        ctx.withVal(TrafficLabel.PROPERTY_KEY, "gray1");

        LabelRoutingResolver.RoutingDecision d =
            router.resolveForReceive(ctx, "T", "G", null, null);

        assertThat(d.getEffectiveGroup()).isEqualTo("G%gray1");
        assertThat(d.getSql92()).isEqualTo("__SERVICE_TAG__ = 'gray1'");
        assertThat(bootstrapper.calledOnceWith("T", "G%gray1")).isTrue();
    }

    @Test
    public void rewriteRegistrationGroup_gray_header_returns_rewritten_group_no_bootstrap() {
        ConfigurationManager.getProxyConfig().setEnableTrafficLabelRouting(true);
        ProxyContext grayCtx = ProxyContext.create();
        grayCtx.withVal(TrafficLabel.PROPERTY_KEY, "gray1");

        ProxyContext stdCtx = ProxyContext.create();

        assertThat(router.rewriteRegistrationGroup(grayCtx, "G")).isEqualTo("G%gray1");
        assertThat(router.rewriteRegistrationGroup(stdCtx, "G")).isEqualTo("G");
        assertThat(bootstrapper.neverCalled()).isTrue();
    }
}
