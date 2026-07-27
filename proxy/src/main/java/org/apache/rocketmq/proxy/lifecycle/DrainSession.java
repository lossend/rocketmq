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

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Immutable per-drain clock. All fixed cutoffs are derived once from a single
 * {@code System.nanoTime()} baseline; wall-clock is retained for logging only.
 * Expiry and remaining-time math use {@code deadline - now} differences so the
 * arithmetic stays correct across {@code nanoTime} wrap-around.
 */
public final class DrainSession {

    private final String drainId;
    private final DrainTrigger trigger;
    private final long startedNanos;
    private final long lbCutoffNanos;
    private final long migrationCutoffNanos;
    private final long hardDeadlineNanos;
    private final long preStopDeadlineNanos;
    private final Instant startedAtForLogOnly;

    private DrainSession(String drainId, DrainTrigger trigger, long startedNanos,
        long lbCutoffNanos, long migrationCutoffNanos, long hardDeadlineNanos,
        long preStopDeadlineNanos, Instant startedAtForLogOnly) {
        this.drainId = drainId;
        this.trigger = trigger;
        this.startedNanos = startedNanos;
        this.lbCutoffNanos = lbCutoffNanos;
        this.migrationCutoffNanos = migrationCutoffNanos;
        this.hardDeadlineNanos = hardDeadlineNanos;
        this.preStopDeadlineNanos = preStopDeadlineNanos;
        this.startedAtForLogOnly = startedAtForLogOnly;
    }

    public static DrainSession fromSeconds(String drainId, DrainTrigger trigger, long startedNanos,
        Instant startedAtForLogOnly, int lbDetachTimeoutSeconds, int connectionLeaseSeconds,
        double remotingLeaseJitterRatio, int connectionLeaseGraceSeconds, int sendDrainTimeoutSeconds,
        int preStopWaitSeconds) {
        long maxLeaseSeconds = (long) Math.ceil(connectionLeaseSeconds * (1.0 + remotingLeaseJitterRatio));
        long lbCutoff = startedNanos + TimeUnit.SECONDS.toNanos(lbDetachTimeoutSeconds);
        long migrationCutoff = lbCutoff
            + TimeUnit.SECONDS.toNanos(maxLeaseSeconds)
            + TimeUnit.SECONDS.toNanos(connectionLeaseGraceSeconds);
        long hardDeadline = migrationCutoff + TimeUnit.SECONDS.toNanos(sendDrainTimeoutSeconds);
        long preStopDeadline = startedNanos + TimeUnit.SECONDS.toNanos(preStopWaitSeconds);
        return new DrainSession(drainId, trigger, startedNanos, lbCutoff, migrationCutoff,
            hardDeadline, preStopDeadline, startedAtForLogOnly);
    }

    private long deadlineFor(DrainPhase phase) {
        switch (phase) {
            case QUIESCE:
                return lbCutoffNanos;
            case MIGRATE:
                return migrationCutoffNanos;
            case DRAIN:
                return hardDeadlineNanos;
            default:
                throw new IllegalArgumentException("unknown drain phase: " + phase);
        }
    }

    /**
     * Remaining nanos before the phase deadline, tightened (never extended) by an
     * optional caller deadline. {@link Long#MAX_VALUE} means "no caller constraint".
     */
    public long remainingNanos(DrainPhase phase, long nowNanos, long callerDeadlineNanos) {
        long phaseRemaining = deadlineFor(phase) - nowNanos;
        long effective = phaseRemaining;
        if (callerDeadlineNanos != Long.MAX_VALUE) {
            long callerRemaining = callerDeadlineNanos - nowNanos;
            effective = Math.min(phaseRemaining, callerRemaining);
        }
        return Math.max(0L, effective);
    }

    public boolean isExpired(DrainPhase phase, long nowNanos) {
        return nowNanos - deadlineFor(phase) >= 0L;
    }

    /**
     * Stop budget capped by both the JVM shutdown timeout and the PreStop-derived
     * ceiling {@code preStopDeadline + jvmTimeout}, floored at zero. Computed via
     * differences to stay wrap-safe.
     */
    public long stopBudgetNanos(long nowNanos, long jvmTimeoutNanos) {
        long preStopCapRemaining = (preStopDeadlineNanos - nowNanos) + jvmTimeoutNanos;
        return Math.max(0L, Math.min(jvmTimeoutNanos, preStopCapRemaining));
    }

    public String drainId() {
        return drainId;
    }

    public DrainTrigger trigger() {
        return trigger;
    }

    public long startedNanos() {
        return startedNanos;
    }

    public long lbCutoffNanos() {
        return lbCutoffNanos;
    }

    public long migrationCutoffNanos() {
        return migrationCutoffNanos;
    }

    public long hardDeadlineNanos() {
        return hardDeadlineNanos;
    }

    public long preStopDeadlineNanos() {
        return preStopDeadlineNanos;
    }

    public Instant startedAtForLogOnly() {
        return startedAtForLogOnly;
    }
}
