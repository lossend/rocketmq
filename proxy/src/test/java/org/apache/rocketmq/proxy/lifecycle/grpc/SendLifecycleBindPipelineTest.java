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

package org.apache.rocketmq.proxy.lifecycle.grpc;

import apache.rocketmq.v2.QueryRouteRequest;
import io.grpc.Context;
import io.grpc.Metadata;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.pipeline.RequestPipeline;
import org.apache.rocketmq.proxy.lifecycle.SendDrainGate;
import org.apache.rocketmq.proxy.lifecycle.SendPermit;
import org.apache.rocketmq.proxy.lifecycle.SendProtocol;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SendLifecycleBindPipelineTest {

    private final SendLifecycleBindPipeline pipeline = new SendLifecycleBindPipeline();
    private final QueryRouteRequest anyRequest = QueryRouteRequest.getDefaultInstance();

    private void runInHolderContext(GrpcSendLifecycleHolder holder, Runnable body) {
        Context ctx = Context.ROOT.withValue(GrpcSendStreamTracerFactory.HOLDER_KEY, holder);
        Context previous = ctx.attach();
        try {
            body.run();
        } finally {
            ctx.detach(previous);
        }
    }

    @Test
    @DisplayName("a bound permit is copied from the grpc Context onto the ProxyContext")
    public void copiesBoundPermit() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit permit = gate.tryAcquire(SendProtocol.GRPC).get();
        GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
        holder.bindPermit(permit);

        ProxyContext context = ProxyContext.create();
        runInHolderContext(holder, () -> pipeline.execute(context, new Metadata(), anyRequest));

        assertThat(context.getSendLifecycleContext()).isSameAs(permit);
    }

    @Test
    @DisplayName("a holder without a permit leaves the ProxyContext untouched")
    public void holderWithoutPermitIsNoop() {
        GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
        ProxyContext context = ProxyContext.create();
        runInHolderContext(holder, () -> pipeline.execute(context, new Metadata(), anyRequest));
        assertThat(context.getSendLifecycleContext()).isNull();
    }

    @Test
    @DisplayName("a non-send RPC has no holder at all, so the stage is inert")
    public void noHolderIsInert() {
        ProxyContext context = ProxyContext.create();
        pipeline.execute(context, new Metadata(), anyRequest);
        assertThat(context.getSendLifecycleContext()).isNull();
    }

    @Test
    @DisplayName("the bind stage runs after the context-init stage in the composed pipeline")
    public void bindRunsAfterContextInit() {
        // Mirrors GrpcMessagingApplication.create(): the bind stage is piped on first,
        // then the context-init stage. RequestPipeline.pipe runs the piped source
        // before the receiver, so the last stage piped on executes first.
        List<String> order = new ArrayList<>();
        RequestPipeline terminal = (ctx, headers, request) -> order.add("terminal");
        RequestPipeline bind = (ctx, headers, request) -> order.add("bind");
        RequestPipeline contextInit = (ctx, headers, request) -> order.add("context-init");

        RequestPipeline composed = terminal.pipe(bind).pipe(contextInit);
        composed.execute(ProxyContext.create(), new Metadata(), anyRequest);

        assertThat(order).containsExactly("context-init", "bind", "terminal");
    }
}
