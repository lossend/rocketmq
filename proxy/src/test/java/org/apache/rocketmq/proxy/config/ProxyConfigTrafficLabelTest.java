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

package org.apache.rocketmq.proxy.config;

import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for traffic-label routing configuration fields in {@link ProxyConfig}.
 */
public class ProxyConfigTrafficLabelTest {

    @Test
    public void should_default_all_traffic_label_toggles_off_for_production_safety() {
        ProxyConfig config = new ProxyConfig();
        assertThat(config.isEnableTrafficLabelRouting()).isFalse();
        assertThat(config.isEnableTrafficLabelGroupCleanup()).isFalse();
        assertThat(config.isEnableTrafficLabelRoutingLog()).isFalse();
    }

    @Test
    public void should_default_cleanup_idle_threshold_to_one_hour() {
        ProxyConfig config = new ProxyConfig();
        assertThat(config.getTrafficLabelGroupCleanupIdleThresholdMs()).isEqualTo(3600_000L);
    }

    @Test
    public void should_allow_enabling_routing() {
        ProxyConfig config = new ProxyConfig();
        config.setEnableTrafficLabelRouting(true);
        assertThat(config.isEnableTrafficLabelRouting()).isTrue();
    }
}
