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

package org.apache.rocketmq.proxy.lifecycle.warmup;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One-time startup gate over a set of {@link WarmupTask}s. {@link #tryCompleteWarmup()} runs
 * every task once and latches {@link #hasCompletedWarmup()} only when all succeed; once
 * latched it stays latched. Purely a startup concern — it does not back any runtime
 * readiness predicate. See {@code org.apache.rocketmq.proxy.lifecycle.readiness.ReadinessEvaluator}
 * for the runtime {@code /ready-for-traffic} predicate, which is evaluated separately from a
 * distinct set of registered probes.
 */
public final class WarmupBarrier {

    private final List<WarmupTask> tasks;

    private final AtomicBoolean readyOnce = new AtomicBoolean(false);

    public WarmupBarrier(List<WarmupTask> tasks) {
        this.tasks = tasks;
    }

    /**
     * Runs every task once. Returns true and latches {@code readyOnce} only when all
     * succeed. Idempotent: once latched it stays latched.
     */
    public boolean tryCompleteWarmup() {
        for (WarmupTask task : tasks) {
            WarmupResult result = join(task);
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

    private static WarmupResult join(WarmupTask task) {
        try {
            return task.warmup().join();
        } catch (Exception e) {
            return WarmupResult.failure(task.name() + ": " + e.getMessage());
        }
    }
}
