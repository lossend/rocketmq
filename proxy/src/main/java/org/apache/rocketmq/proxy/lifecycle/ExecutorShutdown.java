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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Uniform bounded executor teardown: {@code shutdown() -> awaitTermination(remaining)
 * -> shutdownNow()}. Every await consumes only the remaining time of the shared
 * {@link ShutdownDeadline}; no phase gets a fresh budget. Returns the tasks dropped
 * by {@code shutdownNow} so the caller can run their skip hooks. Interrupts are
 * recorded and the flag is restored, never swallowed.
 */
public final class ExecutorShutdown {

    private ExecutorShutdown() {
    }

    /**
     * Result of a bounded shutdown: whether the pool terminated cleanly, whether it
     * was interrupted, and any tasks dropped by the forced stop.
     */
    public static final class Result {
        private final boolean terminated;
        private final boolean interrupted;
        private final List<Runnable> droppedTasks;

        Result(boolean terminated, boolean interrupted, List<Runnable> droppedTasks) {
            this.terminated = terminated;
            this.interrupted = interrupted;
            this.droppedTasks = droppedTasks;
        }

        public boolean terminated() {
            return terminated;
        }

        public boolean interrupted() {
            return interrupted;
        }

        public List<Runnable> droppedTasks() {
            return droppedTasks;
        }
    }

    public static Result shutdown(ExecutorService executor, ShutdownDeadline deadline) {
        if (executor == null) {
            return new Result(true, false, java.util.Collections.emptyList());
        }
        executor.shutdown();
        boolean terminated;
        boolean interrupted = false;
        try {
            long remaining = deadline.remainingNanos();
            terminated = remaining > 0 && executor.awaitTermination(remaining, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            interrupted = true;
            terminated = false;
        }
        if (terminated) {
            return new Result(true, false, java.util.Collections.emptyList());
        }
        List<Runnable> dropped = executor.shutdownNow();
        // Best-effort final await within whatever budget is left.
        try {
            long remaining = deadline.remainingNanos();
            if (remaining > 0) {
                terminated = executor.awaitTermination(remaining, TimeUnit.NANOSECONDS);
            }
        } catch (InterruptedException e) {
            interrupted = true;
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return new Result(terminated, interrupted, dropped);
    }
}
