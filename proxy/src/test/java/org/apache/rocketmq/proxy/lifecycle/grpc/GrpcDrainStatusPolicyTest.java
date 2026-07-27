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
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GrpcDrainStatusPolicyTest {

    private final GrpcDrainStatusPolicy policy = new GrpcDrainStatusPolicy();

    @Test
    @DisplayName("Telemetry is closed with OK so clients take the quiet renewal path")
    public void telemetryClosesOk() {
        String telemetry = MessagingServiceGrpc.getTelemetryMethod().getFullMethodName();
        assertThat(policy.closeStatusFor(telemetry).getCode()).isEqualTo(Status.Code.OK);
        assertThat(policy.isCompletedNormally(telemetry)).isTrue();
    }

    @Test
    @DisplayName("ReceiveMessage is closed with a retryable UNAVAILABLE, not a fake success")
    public void receiveMessageClosesUnavailable() {
        String receive = MessagingServiceGrpc.getReceiveMessageMethod().getFullMethodName();
        Status status = policy.closeStatusFor(receive);
        assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(status.getDescription()).isEqualTo(GrpcDrainStatusPolicy.DRAINING_DESCRIPTION);
        assertThat(policy.isCompletedNormally(receive)).isFalse();
    }

    @Test
    @DisplayName("an unknown future streaming method also gets a retryable UNAVAILABLE")
    public void unknownStreamingClosesUnavailable() {
        Status status = policy.closeStatusFor("apache.rocketmq.v2.MessagingService/FutureStream");
        assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    }
}
