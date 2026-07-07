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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TrafficLabelTest {

    @Test
    public void should_build_virtual_group_for_gray_label() {
        assertThat(TrafficLabel.effectiveGroup("G", "gray1")).isEqualTo("G%gray1");
    }

    @Test
    public void should_return_origin_group_when_label_blank() {
        assertThat(TrafficLabel.effectiveGroup("G", null)).isEqualTo("G");
        assertThat(TrafficLabel.effectiveGroup("G", "")).isEqualTo("G");
        assertThat(TrafficLabel.effectiveGroup("G", "   ")).isEqualTo("G");
    }

    @Test
    public void should_treat_standard_label_as_origin_group() {
        assertThat(TrafficLabel.effectiveGroup("G", "default")).isEqualTo("G");
    }

    @Test
    public void should_detect_gray_label() {
        assertThat(TrafficLabel.isGray("gray1")).isTrue();
        assertThat(TrafficLabel.isGray("default")).isFalse();
        assertThat(TrafficLabel.isGray(null)).isFalse();
        assertThat(TrafficLabel.isGray("")).isFalse();
    }

    @Test
    public void should_parse_logical_group_from_virtual_group() {
        assertThat(TrafficLabel.parseLogicalGroup("G%gray1")).isEqualTo("G");
    }

    @Test
    public void should_parse_label_from_virtual_group() {
        assertThat(TrafficLabel.parseLabel("G%gray1")).isEqualTo("gray1");
    }

    @Test
    public void should_return_whole_group_as_logical_group_when_no_separator() {
        assertThat(TrafficLabel.parseLogicalGroup("G")).isEqualTo("G");
    }

    @Test
    public void should_return_null_label_when_no_separator() {
        assertThat(TrafficLabel.parseLabel("G")).isNull();
    }
}
