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

import org.apache.rocketmq.proxy.lifecycle.SendDrainGate;
import org.apache.rocketmq.proxy.lifecycle.SendPermit;
import org.apache.rocketmq.proxy.lifecycle.SendProtocol;
import org.apache.rocketmq.proxy.lifecycle.SkipReason;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GrpcSendLifecycleHolderTest {

    @Test
    @DisplayName("a permit binds exactly once")
    public void bindOnce() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit permit = gate.tryAcquire(SendProtocol.GRPC).get();
        GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
        assertThat(holder.bindPermit(permit)).isTrue();
        assertThat(holder.bindPermit(permit)).isFalse();
        assertThat(holder.hasPermit()).isTrue();
    }

    @Test
    @DisplayName("streamClosed on a bound-but-never-started permit skips the backend and releases it")
    public void streamClosedSkipsUnstartedBackend() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit permit = gate.tryAcquire(SendProtocol.GRPC).get();
        GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
        holder.bindPermit(permit);

        holder.onStreamClosed(true, null);
        assertThat(permit.completionFuture()).isCompleted();
        assertThat(gate.acceptedCount()).isZero();
    }

    @Test
    @DisplayName("streamClosed after a started+terminal backend writes only the protocol terminal")
    public void streamClosedAfterBackendTerminal() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit permit = gate.tryAcquire(SendProtocol.GRPC).get();
        GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
        holder.bindPermit(permit);

        assertThat(permit.backendStarted()).isTrue();
        permit.backendTerminal(null);
        assertThat(permit.completionFuture()).isNotCompleted();

        holder.onStreamClosed(true, null);
        assertThat(permit.completionFuture()).isCompleted();
    }

    @Test
    @DisplayName("a rejected-before-admission holder has no permit and streamClosed is a no-op")
    public void rejectedBeforeAdmission() {
        GrpcSendLifecycleHolder holder = new GrpcSendLifecycleHolder();
        holder.rejectBeforeAdmission(SkipReason.GATE_CLOSED);
        assertThat(holder.hasPermit()).isFalse();
        assertThat(holder.wasRejectedBeforeAdmission()).isTrue();
        holder.onStreamClosed(false, new RuntimeException("closed"));
    }
}
