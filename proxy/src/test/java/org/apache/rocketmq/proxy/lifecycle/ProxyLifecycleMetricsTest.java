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

package org.apache.rocketmq.proxy.lifecycle;

import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProxyLifecycleMetricsTest {

    @Test
    @DisplayName("counters are readable without binding to a meter, so admin snapshots still work exporter-off")
    public void countersReadableWithoutBinding() {
        ProxyLifecycleMetrics metrics = new ProxyLifecycleMetrics();
        assertThat(metrics.isBound()).isFalse();
        metrics.recordForcedClose();
        metrics.recordForcedClose();
        metrics.recordLateConnectionAfterCutoff();
        assertThat(metrics.forcedClose()).isEqualTo(2);
        assertThat(metrics.lateConnectionsAfterCutoff()).isEqualTo(1);
        assertThat(metrics.drainDeadlineExceeded()).isZero();
    }

    @Test
    @DisplayName("binding to a meter is a once-only operation")
    public void bindOnceOnly() {
        ProxyLifecycleMetrics metrics = new ProxyLifecycleMetrics();
        metrics.bind();
        assertThat(metrics.isBound()).isTrue();
        try {
            metrics.bind();
            org.junit.Assert.fail("expected IllegalStateException on double bind");
        } catch (IllegalStateException expected) {
            assertThat(expected).hasMessageContaining("already bound");
        }
    }

    @Test
    @DisplayName("each counter increments independently")
    public void countersIndependent() {
        ProxyLifecycleMetrics metrics = new ProxyLifecycleMetrics();
        metrics.recordConnectionLeaseExpired();
        metrics.recordDrainDeadlineExceeded();
        metrics.recordTransportTerminationFailed();
        assertThat(metrics.connectionLeaseExpired()).isEqualTo(1);
        assertThat(metrics.drainDeadlineExceeded()).isEqualTo(1);
        assertThat(metrics.transportTerminationFailed()).isEqualTo(1);
        assertThat(metrics.forcedClose()).isZero();
    }
}
