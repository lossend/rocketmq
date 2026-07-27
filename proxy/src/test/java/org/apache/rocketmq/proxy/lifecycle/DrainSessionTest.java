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
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DrainSessionTest {

    private static final long T0 = 1_000_000_000L;

    private static long secs(long s) {
        return TimeUnit.SECONDS.toNanos(s);
    }

    private DrainSession defaultSession(long startedNanos) {
        // Defaults: lbDetach=60, lease=300, jitter=0.10, grace=30, sendDrain=30, preStop=480
        return DrainSession.fromSeconds("drain-1", DrainTrigger.PRESTOP, startedNanos,
            Instant.ofEpochMilli(0), 60, 300, 0.10, 30, 30, 480);
    }

    @Test
    @DisplayName("cutoffs are derived from configured seconds relative to T0")
    public void deriveCutoffsFromSeconds() {
        DrainSession session = defaultSession(T0);
        // maxLease = ceil(300 * 1.10) = 330
        assertThat(session.lbCutoffNanos()).isEqualTo(T0 + secs(60));
        assertThat(session.migrationCutoffNanos()).isEqualTo(T0 + secs(60 + 330 + 30));
        assertThat(session.hardDeadlineNanos()).isEqualTo(T0 + secs(60 + 330 + 30 + 30));
        assertThat(session.preStopDeadlineNanos()).isEqualTo(T0 + secs(480));
    }

    @Test
    @DisplayName("fixed cutoffs are strictly monotonically increasing")
    public void cutoffsStrictlyMonotonic() {
        DrainSession session = defaultSession(T0);
        assertThat(session.startedNanos()).isLessThan(session.lbCutoffNanos());
        assertThat(session.lbCutoffNanos()).isLessThan(session.migrationCutoffNanos());
        assertThat(session.migrationCutoffNanos()).isLessThan(session.hardDeadlineNanos());
        assertThat(session.hardDeadlineNanos()).isLessThanOrEqualTo(session.preStopDeadlineNanos());
    }

    @Test
    @DisplayName("hard deadline never exceeds the PreStop deadline for default budgets")
    public void hardDeadlineWithinPreStop() {
        DrainSession session = defaultSession(T0);
        assertThat(session.hardDeadlineNanos()).isLessThanOrEqualTo(session.preStopDeadlineNanos());
    }

    @Test
    @DisplayName("remaining time for a phase is the phase deadline minus now, floored at zero")
    public void remainingForPhase() {
        DrainSession session = defaultSession(T0);
        long now = T0 + secs(10);
        assertThat(session.remainingNanos(DrainPhase.QUIESCE, now, Long.MAX_VALUE))
            .isEqualTo(secs(60) - secs(10));
        // past the deadline -> zero, never negative
        assertThat(session.remainingNanos(DrainPhase.QUIESCE, T0 + secs(120), Long.MAX_VALUE))
            .isZero();
    }

    @Test
    @DisplayName("caller deadline only tightens the phase deadline, never extends it")
    public void callerDeadlineTightensOnly() {
        DrainSession session = defaultSession(T0);
        long now = T0;
        long callerEarlier = T0 + secs(5);
        assertThat(session.remainingNanos(DrainPhase.DRAIN, now, callerEarlier)).isEqualTo(secs(5));
        long callerLater = session.hardDeadlineNanos() + secs(1000);
        assertThat(session.remainingNanos(DrainPhase.DRAIN, now, callerLater))
            .isEqualTo(session.hardDeadlineNanos() - now);
    }

    @Test
    @DisplayName("a phase is expired once now reaches or passes its deadline")
    public void phaseExpiry() {
        DrainSession session = defaultSession(T0);
        assertThat(session.isExpired(DrainPhase.QUIESCE, session.lbCutoffNanos() - 1)).isFalse();
        assertThat(session.isExpired(DrainPhase.QUIESCE, session.lbCutoffNanos())).isTrue();
        assertThat(session.isExpired(DrainPhase.DRAIN, session.hardDeadlineNanos() + 1)).isTrue();
    }

    @Test
    @DisplayName("stop budget is capped by both the JVM timeout and the PreStop-derived cap")
    public void stopBudgetDualCap() {
        DrainSession session = defaultSession(T0);
        long jvm = secs(30);
        // Early: JVM timeout is the tighter cap.
        assertThat(session.stopBudgetNanos(T0, jvm)).isEqualTo(jvm);
        // Near the PreStop deadline: the (preStop + jvm - now) cap wins and stays non-negative.
        long nearEnd = session.preStopDeadlineNanos() + secs(10);
        assertThat(session.stopBudgetNanos(nearEnd, jvm)).isEqualTo(secs(20));
        long past = session.preStopDeadlineNanos() + jvm + secs(5);
        assertThat(session.stopBudgetNanos(past, jvm)).isZero();
    }

    @Test
    @DisplayName("deadline math stays correct when nanoTime is near Long.MAX_VALUE")
    public void nanoTimeWrapAround() {
        long startedNanos = Long.MAX_VALUE - secs(100);
        DrainSession session = defaultSession(startedNanos);
        // lbCutoff has wrapped past Long.MAX_VALUE; remaining must still be ~60s at T0.
        long remaining = session.remainingNanos(DrainPhase.QUIESCE, startedNanos, Long.MAX_VALUE);
        assertThat(remaining).isEqualTo(secs(60));
        assertThat(session.isExpired(DrainPhase.QUIESCE, startedNanos)).isFalse();
    }

    @Test
    @DisplayName("session identity fields are preserved")
    public void identityFields() {
        DrainSession session = defaultSession(T0);
        assertThat(session.drainId()).isEqualTo("drain-1");
        assertThat(session.trigger()).isEqualTo(DrainTrigger.PRESTOP);
        assertThat(session.startedNanos()).isEqualTo(T0);
    }
}
