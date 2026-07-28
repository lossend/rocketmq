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

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.pipeline.RequestPipeline;

/**
 * Copies the per-stream send permit from the gRPC {@link io.grpc.Context} onto the
 * {@link ProxyContext}. This must run on the service thread, because the tracer's
 * holder lives in the gRPC Context and is not visible from the worker thread that
 * later executes the request.
 * <p>
 * Non-send RPCs carry no holder, so this is a no-op for them. When the graceful
 * lifecycle is disabled no holder is ever created, so the pipeline stage is inert.
 */
public class SendLifecycleBindPipeline implements RequestPipeline {

    @Override
    public void execute(ProxyContext context, Metadata headers, GeneratedMessageV3 request) {
        GrpcSendLifecycleHolder holder = GrpcSendStreamTracerFactory.HOLDER_KEY.get();
        if (holder != null && holder.hasPermit()) {
            context.setSendLifecycleContext(holder.permit());
        }
    }
}
