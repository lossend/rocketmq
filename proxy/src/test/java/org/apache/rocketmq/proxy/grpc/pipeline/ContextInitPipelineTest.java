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
package org.apache.rocketmq.proxy.grpc.pipeline;

import io.grpc.Metadata;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.consumer.TrafficLabel;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ContextInitPipeline} — specifically the traffic-label header
 * round-trip: the gRPC metadata header {@code __SERVICE_TAG__} (normalized to
 * {@code __service_tag__} on the wire by grpc-java) must be copied into
 * {@link ProxyContext} under {@link TrafficLabel#PROPERTY_KEY}.
 */
public class ContextInitPipelineTest {

    private final ContextInitPipeline pipeline = new ContextInitPipeline();

    /**
     * Verifies that a gray traffic label sent in the gRPC metadata header is
     * stored in ProxyContext so routing logic can read it via
     * {@link TrafficLabel#PROPERTY_KEY}.
     */
    @Test
    public void should_copy_traffic_label_header_into_proxy_context() {
        Metadata headers = new Metadata();
        headers.put(GrpcConstants.TRAFFIC_LABEL, "gray1");

        ProxyContext ctx = ProxyContext.create();
        pipeline.execute(ctx, headers, null);

        assertThat((String) ctx.getVal(TrafficLabel.PROPERTY_KEY)).isEqualTo("gray1");
    }

    /**
     * Verifies that a blank traffic-label header is not stored — the routing
     * logic treats absent context values as the standard (non-gray) lane.
     */
    @Test
    public void should_not_store_blank_traffic_label_header() {
        Metadata headers = new Metadata();
        headers.put(GrpcConstants.TRAFFIC_LABEL, "   ");

        ProxyContext ctx = ProxyContext.create();
        pipeline.execute(ctx, headers, null);

        assertThat((String) ctx.getVal(TrafficLabel.PROPERTY_KEY)).isNull();
    }

    /**
     * Verifies that when no traffic-label header is present, the ProxyContext
     * entry is absent (standard lane behaviour).
     */
    @Test
    public void should_not_store_anything_when_traffic_label_header_absent() {
        Metadata headers = new Metadata();

        ProxyContext ctx = ProxyContext.create();
        pipeline.execute(ctx, headers, null);

        assertThat((String) ctx.getVal(TrafficLabel.PROPERTY_KEY)).isNull();
    }
}
