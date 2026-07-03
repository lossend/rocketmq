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

import org.apache.rocketmq.common.filter.ExpressionType;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LabelRoutingResolverTest {

    private final LabelRoutingResolver resolver = new LabelRoutingResolver();

    // -----------------------------------------------------------------------
    // Existing cases — updated to 4-arg signature
    // -----------------------------------------------------------------------

    @Test
    public void gray_label_routes_to_virtual_group_with_label_filter() {
        LabelRoutingResolver.RoutingDecision d = resolver.resolve("G", "gray1", null, ExpressionType.TAG);
        assertThat(d.getEffectiveGroup()).isEqualTo("G%gray1");
        assertThat(d.getSql92()).isEqualTo("__service.tag__ = 'gray1'");
    }

    @Test
    public void standard_consumer_gets_complement_filter_on_origin_group() {
        LabelRoutingResolver.RoutingDecision d = resolver.resolve("G", null, null, ExpressionType.TAG);
        assertThat(d.getEffectiveGroup()).isEqualTo("G");
        assertThat(d.getSql92())
            .isEqualTo("__service.tag__ IS NULL OR __service.tag__ = 'default'");
    }

    @Test
    public void gray_label_AND_merges_consumer_sql92_expression() {
        LabelRoutingResolver.RoutingDecision d = resolver.resolve("G", "gray1", "a > 1", ExpressionType.SQL92);
        assertThat(d.getEffectiveGroup()).isEqualTo("G%gray1");
        assertThat(d.getSql92()).isEqualTo("( a > 1 ) AND ( __service.tag__ = 'gray1' )");
    }

    @Test
    public void standard_AND_merges_consumer_sql92_expression() {
        LabelRoutingResolver.RoutingDecision d = resolver.resolve("G", "default", "a > 1", ExpressionType.SQL92);
        assertThat(d.getEffectiveGroup()).isEqualTo("G");
        assertThat(d.getSql92())
            .isEqualTo("( a > 1 ) AND ( __service.tag__ IS NULL OR __service.tag__ = 'default' )");
    }

    @Test
    public void gray_label_matches_message_with_only_service_tag_key() {
        LabelRoutingResolver.RoutingDecision d = resolver.resolve("G", "gray1", null, ExpressionType.TAG);
        assertThat(d.getSql92()).contains("__service.tag__ = 'gray1'");
    }

    // -----------------------------------------------------------------------
    // Regression: SUB_ALL and TAG filter conversion
    // -----------------------------------------------------------------------

    /**
     * Regression: the default SimpleConsumer subscription sends "*" with type TAG.
     * Before the fix this produced "( * ) AND ( ... )" — invalid SQL92 rejected by the broker
     * with CODE 23. Now it must produce only the label condition.
     */
    @Test
    public void sub_all_star_yields_label_only_for_gray() {
        LabelRoutingResolver.RoutingDecision d = resolver.resolve("G", "gray1", "*", ExpressionType.TAG);
        assertThat(d.getSql92()).isEqualTo("__service.tag__ = 'gray1'");
    }

    @Test
    public void sub_all_star_yields_label_only_for_standard() {
        LabelRoutingResolver.RoutingDecision d = resolver.resolve("G", null, "*", ExpressionType.TAG);
        assertThat(d.getSql92()).isEqualTo("__service.tag__ IS NULL OR __service.tag__ = 'default'");
    }

    @Test
    public void single_tag_converted_to_TAGS_in_for_gray() {
        LabelRoutingResolver.RoutingDecision d = resolver.resolve("G", "gray1", "TagA", ExpressionType.TAG);
        assertThat(d.getSql92()).isEqualTo("( TAGS in ('TagA') ) AND ( __service.tag__ = 'gray1' )");
    }

    @Test
    public void multi_tag_converted_to_TAGS_in_for_gray() {
        LabelRoutingResolver.RoutingDecision d = resolver.resolve("G", "gray1", "TagA || TagB", ExpressionType.TAG);
        assertThat(d.getSql92())
            .isEqualTo("( TAGS in ('TagA', 'TagB') ) AND ( __service.tag__ = 'gray1' )");
    }

    @Test
    public void multi_tag_converted_to_TAGS_in_for_standard() {
        LabelRoutingResolver.RoutingDecision d = resolver.resolve("G", "default", "TagA", ExpressionType.TAG);
        assertThat(d.getSql92())
            .isEqualTo("( TAGS in ('TagA') ) AND ( __service.tag__ IS NULL OR __service.tag__ = 'default' )");
    }

    @Test
    public void sql92_origin_preserved_unchanged() {
        LabelRoutingResolver.RoutingDecision d = resolver.resolve("G", "gray1", "a > 1", ExpressionType.SQL92);
        assertThat(d.getSql92()).isEqualTo("( a > 1 ) AND ( __service.tag__ = 'gray1' )");
    }
}
