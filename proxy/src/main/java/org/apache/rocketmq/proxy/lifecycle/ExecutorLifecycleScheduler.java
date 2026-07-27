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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * {@link LifecycleScheduler} backed by a single-thread
 * {@link ScheduledExecutorService}. Drain orchestration never blocks the
 * lifecycle thread; phase timers are cancellable scheduled futures.
 */
public final class ExecutorLifecycleScheduler implements LifecycleScheduler {

    private final ScheduledExecutorService executor;

    public ExecutorLifecycleScheduler(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }

    @Override
    public ScheduledHandle schedule(Runnable task, long delayNanos) {
        ScheduledFuture<?> future = executor.schedule(task, Math.max(0L, delayNanos), TimeUnit.NANOSECONDS);
        return () -> future.cancel(false);
    }
}
