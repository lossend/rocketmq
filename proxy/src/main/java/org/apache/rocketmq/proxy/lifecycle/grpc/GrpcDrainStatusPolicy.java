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

import apache.rocketmq.v2.MessagingServiceGrpc;
import io.grpc.Status;

/**
 * Chooses the close status for a non-unary RPC when the drain proactively ends
 * it. Telemetry gets {@link Status#OK} so 5.0.7/5.2.1 clients take their quiet
 * 1-second observer renewal path; every other non-unary RPC (ReceiveMessage,
 * PullMessage, and any future streaming method) gets a retryable
 * {@code UNAVAILABLE} so a truncated business stream is never disguised as
 * success.
 */
public final class GrpcDrainStatusPolicy {

    public static final String DRAINING_DESCRIPTION = "[PROXY_DRAINING] reconnect";

    private final String telemetryMethod;

    public GrpcDrainStatusPolicy() {
        this(MessagingServiceGrpc.getTelemetryMethod().getFullMethodName());
    }

    GrpcDrainStatusPolicy(String telemetryMethod) {
        this.telemetryMethod = telemetryMethod;
    }

    public Status closeStatusFor(String fullMethodName) {
        if (telemetryMethod.equals(fullMethodName)) {
            return Status.OK;
        }
        return Status.UNAVAILABLE.withDescription(DRAINING_DESCRIPTION);
    }

    public boolean isCompletedNormally(String fullMethodName) {
        return telemetryMethod.equals(fullMethodName);
    }
}
