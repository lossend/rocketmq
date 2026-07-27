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
import io.grpc.ServerTransportFilter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/**
 * Tags each transport with its connection id, ready time, and a late flag when it
 * arrives after the drain's lb cutoff. The public {@link ServerTransportFilter}
 * cannot close an individual transport, so a late transport only has its send RPCs
 * rejected by the interceptor; final termination comes from max-age or server
 * shutdown. {@code transportTerminated} only updates the connection count.
 */
public final class GrpcTransportLifecycleFilter extends ServerTransportFilter {

    public static final Attributes.Key<Long> CONNECTION_ID =
        Attributes.Key.create("proxy-connection-id");
    public static final Attributes.Key<Long> READY_NANOS =
        Attributes.Key.create("proxy-ready-nanos");
    public static final Attributes.Key<Boolean> LATE_AFTER_LB_CUTOFF =
        Attributes.Key.create("proxy-late-after-lb-cutoff");

    private final LongSupplier nanoClock;
    private final LongSupplier lbCutoffNanosSupplier;
    private final AtomicInteger idSeq = new AtomicInteger();
    private final AtomicInteger connectionCount = new AtomicInteger();

    public GrpcTransportLifecycleFilter(LongSupplier nanoClock, LongSupplier lbCutoffNanosSupplier) {
        this.nanoClock = nanoClock;
        this.lbCutoffNanosSupplier = lbCutoffNanosSupplier;
    }

    /** True when {@code readyNanos} is at or after the current lb cutoff (0 = no cutoff yet). */
    boolean isLate(long readyNanos, long lbCutoffNanos) {
        return lbCutoffNanos != 0L && readyNanos - lbCutoffNanos >= 0L;
    }

    @Override
    public Attributes transportReady(Attributes transportAttrs) {
        long now = nanoClock.getAsLong();
        boolean late = isLate(now, lbCutoffNanosSupplier.getAsLong());
        connectionCount.incrementAndGet();
        return transportAttrs.toBuilder()
            .set(CONNECTION_ID, (long) idSeq.incrementAndGet())
            .set(READY_NANOS, now)
            .set(LATE_AFTER_LB_CUTOFF, late)
            .build();
    }

    @Override
    public void transportTerminated(Attributes transportAttrs) {
        connectionCount.decrementAndGet();
    }

    public int connectionCount() {
        return connectionCount.get();
    }
}
