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

package org.apache.rocketmq.proxy.lifecycle.readiness;

import java.util.List;
import java.util.function.BooleanSupplier;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.apache.rocketmq.proxy.lifecycle.ReadinessResult;

/**
 * Evaluates the {@code /ready-for-traffic} provider-health predicate from a live set of
 * {@link ReadinessProbe}s. Re-runs every probe on each call, unlike the one-time startup
 * warmup latch: {@code LOCAL_FATAL} fails immediately, {@code SHARED_DEPENDENCY} fails-close
 * once the consecutive-failure threshold is reached.
 *
 * <p>Gated behind {@code warmupCompleted}: traffic is never signaled ready before the startup
 * warmup barrier has latched at least once. A local fatal is sticky; once marked, this
 * evaluator never reports ready-for-traffic again.
 */
public final class ReadinessEvaluator {

    private final List<ReadinessProbe> probes;
    private final int dependencyFailureThreshold;
    private final BooleanSupplier warmupCompleted;

    private volatile boolean localFatal = false;

    /**
     * Creates the evaluator.
     *
     * @param probes                     the registered runtime readiness probes
     * @param dependencyFailureThreshold consecutive SHARED_DEPENDENCY failures tolerated
     *                                   before {@code /ready-for-traffic} fails closed
     * @param warmupCompleted            reports whether startup warmup has latched
     */
    public ReadinessEvaluator(List<ReadinessProbe> probes, int dependencyFailureThreshold,
        BooleanSupplier warmupCompleted) {
        this.probes = probes;
        this.dependencyFailureThreshold = dependencyFailureThreshold;
        this.warmupCompleted = warmupCompleted;
    }

    /** Sticky: once marked, {@link #isReadyForTraffic()} never reports ready again. */
    public void markLocalFatal() {
        localFatal = true;
    }

    /** Provider health predicate: fails immediately on LOCAL_FATAL, fails-close on threshold. */
    public boolean isReadyForTraffic() {
        if (!warmupCompleted.getAsBoolean() || localFatal) {
            return false;
        }
        int sharedDependencyFailures = 0;
        for (ReadinessProbe probe : probes) {
            ReadinessResult result = join(probe);
            if (result.isSuccess()) {
                continue;
            }
            if (probe.failureScope() == FailureScope.LOCAL_FATAL) {
                return false;
            }
            sharedDependencyFailures++;
        }
        return sharedDependencyFailures < dependencyFailureThreshold;
    }

    private static ReadinessResult join(ReadinessProbe probe) {
        try {
            return probe.check().join();
        } catch (Exception e) {
            return ReadinessResult.failure(probe.name() + ": " + e.getMessage());
        }
    }
}
