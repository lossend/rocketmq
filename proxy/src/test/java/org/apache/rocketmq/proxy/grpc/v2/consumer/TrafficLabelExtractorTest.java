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

import org.apache.rocketmq.proxy.common.ProxyContext;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TrafficLabelExtractor}.
 */
public class TrafficLabelExtractorTest {

    @Test
    public void returns_label_when_present_in_context() {
        ProxyContext ctx = ProxyContext.create();
        ctx.withVal(TrafficLabel.PROPERTY_KEY, "gray1");
        assertThat(TrafficLabelExtractor.extract(ctx)).isEqualTo("gray1");
    }

    @Test
    public void returns_null_when_absent() {
        ProxyContext ctx = ProxyContext.create();
        assertThat(TrafficLabelExtractor.extract(ctx)).isNull();
    }

    @Test
    public void returns_null_when_context_is_null() {
        assertThat(TrafficLabelExtractor.extract(null)).isNull();
    }
}
