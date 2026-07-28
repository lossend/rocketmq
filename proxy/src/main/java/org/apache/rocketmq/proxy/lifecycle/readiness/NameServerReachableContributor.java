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

import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.apache.rocketmq.proxy.lifecycle.ReadinessContributor;
import org.apache.rocketmq.proxy.lifecycle.ReadinessResult;

/**
 * Readiness contributor that actively probes NameServer reachability (e.g. via
 * {@code getBrokerClusterInfo}). Because a shared dependency being briefly
 * unreachable must not fail {@code /ready} after warmup, its {@link FailureScope}
 * is {@link FailureScope#SHARED_DEPENDENCY}.
 *
 * <p>The supplied probe MUST be client-side time-bounded (the NameServer RPC takes
 * an explicit timeout), so the check cannot block the single-threaded lifecycle
 * scheduler that drives the warmup loop indefinitely. A probe returning false or
 * throwing is reported as unreachable.
 */
public final class NameServerReachableContributor implements ReadinessContributor {

    /** Stable name used in warmup logs; also referenced by tests. */
    public static final String NAME = "namesrv-reachable";

    private final BooleanSupplier probe;

    /**
     * Creates the contributor.
     *
     * @param probe returns true when NameServer answered within its client-side
     *              timeout; must not block unbounded
     */
    public NameServerReachableContributor(BooleanSupplier probe) {
        this.probe = probe;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public CompletableFuture<ReadinessResult> check() {
        try {
            return CompletableFuture.completedFuture(probe.getAsBoolean()
                ? ReadinessResult.success()
                : ReadinessResult.failure(NAME + " unreachable"));
        } catch (Exception e) {
            return CompletableFuture.completedFuture(ReadinessResult.failure(NAME + ": " + e.getMessage()));
        }
    }

    @Override
    public FailureScope failureScope() {
        return FailureScope.SHARED_DEPENDENCY;
    }
}
