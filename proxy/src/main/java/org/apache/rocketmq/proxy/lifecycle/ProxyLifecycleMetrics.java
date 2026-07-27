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

import java.util.concurrent.atomic.LongAdder;

/**
 * Per-runtime holder of low-cardinality lifecycle counters. It owns only plain
 * atomic data so the admin snapshot can read it even when the metrics exporter
 * is disabled; binding to a meter is a separate, once-only step done by the
 * metrics manager. No process-static mutable state is added.
 */
public final class ProxyLifecycleMetrics {

    private final LongAdder lateConnectionsAfterCutoff = new LongAdder();
    private final LongAdder connectionLeaseExpired = new LongAdder();
    private final LongAdder drainDeadlineExceeded = new LongAdder();
    private final LongAdder transportTerminationFailed = new LongAdder();
    private final LongAdder forcedClose = new LongAdder();

    private volatile boolean bound = false;

    /** Marks the meter bound exactly once; a second bind is rejected. */
    public synchronized void bind() {
        if (bound) {
            throw new IllegalStateException("ProxyLifecycleMetrics already bound to a meter");
        }
        bound = true;
    }

    public boolean isBound() {
        return bound;
    }

    public void recordLateConnectionAfterCutoff() {
        lateConnectionsAfterCutoff.increment();
    }

    public void recordConnectionLeaseExpired() {
        connectionLeaseExpired.increment();
    }

    public void recordDrainDeadlineExceeded() {
        drainDeadlineExceeded.increment();
    }

    public void recordTransportTerminationFailed() {
        transportTerminationFailed.increment();
    }

    public void recordForcedClose() {
        forcedClose.increment();
    }

    public long lateConnectionsAfterCutoff() {
        return lateConnectionsAfterCutoff.sum();
    }

    public long connectionLeaseExpired() {
        return connectionLeaseExpired.sum();
    }

    public long drainDeadlineExceeded() {
        return drainDeadlineExceeded.sum();
    }

    public long transportTerminationFailed() {
        return transportTerminationFailed.sum();
    }

    public long forcedClose() {
        return forcedClose.sum();
    }
}
