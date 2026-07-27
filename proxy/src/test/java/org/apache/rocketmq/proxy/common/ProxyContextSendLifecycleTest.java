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

package org.apache.rocketmq.proxy.common;

import org.apache.rocketmq.proxy.lifecycle.SendDrainGate;
import org.apache.rocketmq.proxy.lifecycle.SendLifecycleContext;
import org.apache.rocketmq.proxy.lifecycle.SendPermit;
import org.apache.rocketmq.proxy.lifecycle.SendProtocol;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProxyContextSendLifecycleTest {

    @Test
    @DisplayName("the send lifecycle permit round-trips through ProxyContext")
    public void roundTripsPermit() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit permit = gate.tryAcquire(SendProtocol.GRPC).get();
        ProxyContext context = ProxyContext.create();
        assertThat(context.getSendLifecycleContext()).isNull();

        context.setSendLifecycleContext(permit);
        SendLifecycleContext read = context.getSendLifecycleContext();
        assertThat(read).isSameAs(permit);
    }

    @Test
    @DisplayName("a permit skipped before dispatch reports backendStarted false so the Broker is not called")
    public void skippedBeforeDispatchGatesBroker() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit permit = gate.tryAcquire(SendProtocol.GRPC).get();
        ProxyContext context = ProxyContext.create();
        context.setSendLifecycleContext(permit);

        // simulate a reject/cancel before the queued runnable runs
        assertThat(permit.tryBackendSkipped(
            org.apache.rocketmq.proxy.lifecycle.SkipReason.EXECUTOR_REJECTED)).isTrue();

        // the queued runnable's guard: backendStarted() must now be false
        assertThat(context.getSendLifecycleContext().backendStarted()).isFalse();
    }

    @Test
    @DisplayName("a normally started send permit allows the Broker call exactly once")
    public void startedPermitAllowsBrokerOnce() {
        SendDrainGate gate = new SendDrainGate();
        SendPermit permit = gate.tryAcquire(SendProtocol.GRPC).get();
        ProxyContext context = ProxyContext.create();
        context.setSendLifecycleContext(permit);

        assertThat(context.getSendLifecycleContext().backendStarted()).isTrue();
        // second call returns false: the Broker is only dispatched once
        assertThat(context.getSendLifecycleContext().backendStarted()).isFalse();
    }
}
