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

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.apache.rocketmq.proxy.lifecycle.ReadinessContributor;
import org.apache.rocketmq.proxy.lifecycle.ReadinessResult;

/**
 * Bridges a {@link WarmupTask} to the lifecycle {@link ReadinessContributor} the existing
 * {@code ReadinessBarrier} evaluates. {@link #check()} delegates to the task's
 * {@link WarmupTask#warmup()} and maps its {@link WarmupResult} to a {@link ReadinessResult};
 * name and failure scope pass through unchanged.
 */
public final class WarmupTaskContributorAdapter implements ReadinessContributor {

    private final WarmupTask task;

    public WarmupTaskContributorAdapter(WarmupTask task) {
        this.task = task;
    }

    @Override
    public String name() {
        return task.name();
    }

    @Override
    public CompletableFuture<ReadinessResult> check() {
        return task.warmup().thenApply(result -> result.isSuccess()
            ? ReadinessResult.success()
            : ReadinessResult.failure(result.detail()));
    }

    @Override
    public FailureScope failureScope() {
        return task.failureScope();
    }
}
