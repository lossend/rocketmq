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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Evaluates readiness contributors. The one-time warmup barrier requires every
 * contributor to succeed once before {@link #markReadyOnce()} flips to true.
 * After that, the two health predicates diverge:
 * <ul>
 *   <li>{@code isReady} (kubelet/EndpointSlice) fails only on LOCAL_FATAL; a
 *       SHARED_DEPENDENCY blip is fail-open.</li>
 *   <li>{@code isReadyForTraffic} (provider health) additionally fails-close on
 *       SHARED_DEPENDENCY once the failure threshold is reached.</li>
 * </ul>
 * A local fatal is sticky; the barrier never re-opens after it is set.
 */
public final class ReadinessBarrier {

    private final List<ReadinessContributor> contributors;
    private final int dependencyFailureThreshold;

    private final AtomicBoolean readyOnce = new AtomicBoolean(false);
    private volatile boolean localFatal = false;

    public ReadinessBarrier(List<ReadinessContributor> contributors, int dependencyFailureThreshold) {
        this.contributors = contributors;
        this.dependencyFailureThreshold = dependencyFailureThreshold;
    }

    /**
     * Runs every contributor once. Returns true and latches {@code readyOnce} only
     * when all succeed. Idempotent: once latched it stays latched.
     */
    public boolean tryCompleteWarmup() {
        for (ReadinessContributor contributor : contributors) {
            ReadinessResult result = join(contributor);
            if (!result.isSuccess()) {
                return false;
            }
        }
        readyOnce.set(true);
        return true;
    }

    public boolean hasCompletedWarmup() {
        return readyOnce.get();
    }

    public void markLocalFatal() {
        localFatal = true;
    }

    /** kubelet/EndpointSlice predicate: fail-open on shared dependencies. */
    public boolean isReady() {
        if (!readyOnce.get() || localFatal) {
            return false;
        }
        return !anyLocalFatalContributor();
    }

    /** provider health predicate: additionally fail-close on shared-dependency failures. */
    public boolean isReadyForTraffic() {
        if (!readyOnce.get() || localFatal) {
            return false;
        }
        int consecutiveSharedFailures = 0;
        for (ReadinessContributor contributor : contributors) {
            ReadinessResult result = join(contributor);
            if (result.isSuccess()) {
                continue;
            }
            if (contributor.failureScope() == FailureScope.LOCAL_FATAL) {
                return false;
            }
            consecutiveSharedFailures++;
        }
        return consecutiveSharedFailures < dependencyFailureThreshold;
    }

    private boolean anyLocalFatalContributor() {
        for (ReadinessContributor contributor : contributors) {
            if (contributor.failureScope() != FailureScope.LOCAL_FATAL) {
                continue;
            }
            if (!join(contributor).isSuccess()) {
                return true;
            }
        }
        return false;
    }

    private static ReadinessResult join(ReadinessContributor contributor) {
        try {
            return contributor.check().join();
        } catch (Exception e) {
            return ReadinessResult.failure(contributor.name() + ": " + e.getMessage());
        }
    }

    boolean markReadyOnce() {
        return readyOnce.compareAndSet(false, true);
    }
}
