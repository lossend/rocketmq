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

import io.grpc.Attributes;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GrpcTransportLifecycleFilterTest {

    @Test
    @DisplayName("a transport before the lb cutoff is not marked late")
    public void notLateBeforeCutoff() {
        AtomicLong now = new AtomicLong(100);
        AtomicLong cutoff = new AtomicLong(200);
        GrpcTransportLifecycleFilter filter = new GrpcTransportLifecycleFilter(now::get, cutoff::get);
        Attributes attrs = filter.transportReady(Attributes.EMPTY);
        assertThat(attrs.get(GrpcTransportLifecycleFilter.LATE_AFTER_LB_CUTOFF)).isFalse();
        assertThat(attrs.get(GrpcTransportLifecycleFilter.CONNECTION_ID)).isEqualTo(1L);
        assertThat(filter.connectionCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("a transport at or after the lb cutoff is marked late")
    public void lateAfterCutoff() {
        AtomicLong now = new AtomicLong(300);
        AtomicLong cutoff = new AtomicLong(200);
        GrpcTransportLifecycleFilter filter = new GrpcTransportLifecycleFilter(now::get, cutoff::get);
        Attributes attrs = filter.transportReady(Attributes.EMPTY);
        assertThat(attrs.get(GrpcTransportLifecycleFilter.LATE_AFTER_LB_CUTOFF)).isTrue();
    }

    @Test
    @DisplayName("a zero cutoff means no drain is active, so nothing is late")
    public void zeroCutoffNeverLate() {
        AtomicLong now = new AtomicLong(999);
        AtomicLong cutoff = new AtomicLong(0);
        GrpcTransportLifecycleFilter filter = new GrpcTransportLifecycleFilter(now::get, cutoff::get);
        Attributes attrs = filter.transportReady(Attributes.EMPTY);
        assertThat(attrs.get(GrpcTransportLifecycleFilter.LATE_AFTER_LB_CUTOFF)).isFalse();
    }

    @Test
    @DisplayName("quiet duration is -1 until a business transport is observed")
    public void quietUndefinedBeforeAnyConnect() {
        AtomicLong now = new AtomicLong(100);
        GrpcTransportLifecycleFilter filter = new GrpcTransportLifecycleFilter(now::get, () -> 0L);
        assertThat(filter.hasObservedBusinessConnect()).isFalse();
        assertThat(filter.quietDurationNanos()).isEqualTo(-1L);
    }

    @Test
    @DisplayName("quiet duration measures elapsed time since the last business connect")
    public void quietMeasuresSinceLastConnect() {
        AtomicLong now = new AtomicLong(1_000);
        GrpcTransportLifecycleFilter filter = new GrpcTransportLifecycleFilter(now::get, () -> 0L);
        filter.transportReady(Attributes.EMPTY);
        assertThat(filter.hasObservedBusinessConnect()).isTrue();
        assertThat(filter.lastBusinessConnectNanos()).isEqualTo(1_000L);

        now.set(1_500);
        assertThat(filter.quietDurationNanos()).isEqualTo(500L);

        // a newer connect resets the quiet window
        filter.transportReady(Attributes.EMPTY);
        assertThat(filter.quietDurationNanos()).isZero();
    }

    @Test
    @DisplayName("a closed transport does not reset the quiet window")
    public void terminationDoesNotResetQuiet() {
        AtomicLong now = new AtomicLong(1_000);
        GrpcTransportLifecycleFilter filter = new GrpcTransportLifecycleFilter(now::get, () -> 0L);
        Attributes attrs = filter.transportReady(Attributes.EMPTY);
        now.set(2_000);
        filter.transportTerminated(attrs);
        // quiet is measured from the last *connect*, not the last close
        assertThat(filter.quietDurationNanos()).isEqualTo(1_000L);
    }

    @Test
    @DisplayName("transportTerminated decrements the connection count")
    public void terminatedDecrements() {
        AtomicLong now = new AtomicLong(100);
        AtomicLong cutoff = new AtomicLong(0);
        GrpcTransportLifecycleFilter filter = new GrpcTransportLifecycleFilter(now::get, cutoff::get);
        Attributes attrs = filter.transportReady(Attributes.EMPTY);
        assertThat(filter.connectionCount()).isEqualTo(1);
        filter.transportTerminated(attrs);
        assertThat(filter.connectionCount()).isZero();
    }
}
