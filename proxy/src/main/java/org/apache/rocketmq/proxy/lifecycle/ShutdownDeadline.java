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

import java.util.function.LongSupplier;

/**
 * Immutable absolute deadline. All nested owners consume the remaining time of a
 * single instance; no component may rebuild a fresh deadline from a duration.
 * Remaining time is a {@code deadline - now} difference, so it stays wrap-safe.
 */
public final class ShutdownDeadline {

    private final long deadlineNanos;
    private final LongSupplier nanoClock;

    public ShutdownDeadline(long deadlineNanos, LongSupplier nanoClock) {
        this.deadlineNanos = deadlineNanos;
        this.nanoClock = nanoClock;
    }

    public static ShutdownDeadline afterNanos(long nowNanos, long budgetNanos, LongSupplier nanoClock) {
        return new ShutdownDeadline(nowNanos + budgetNanos, nanoClock);
    }

    public long remainingNanos() {
        return Math.max(0L, deadlineNanos - nanoClock.getAsLong());
    }

    public boolean isExpired() {
        return nanoClock.getAsLong() - deadlineNanos >= 0L;
    }

    public long deadlineNanos() {
        return deadlineNanos;
    }
}
