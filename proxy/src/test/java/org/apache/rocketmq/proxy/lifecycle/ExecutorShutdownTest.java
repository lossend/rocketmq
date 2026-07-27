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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ExecutorShutdownTest {

    private ShutdownDeadline deadline(long budgetNanos) {
        return ShutdownDeadline.afterNanos(System.nanoTime(), budgetNanos, System::nanoTime);
    }

    @Test
    @DisplayName("an idle pool terminates cleanly within the deadline")
    public void idlePoolTerminatesCleanly() {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        ExecutorShutdown.Result result = ExecutorShutdown.shutdown(pool,
            deadline(TimeUnit.SECONDS.toNanos(5)));
        assertThat(result.terminated()).isTrue();
        assertThat(result.interrupted()).isFalse();
        assertThat(result.droppedTasks()).isEmpty();
    }

    @Test
    @DisplayName("a null executor is treated as already terminated")
    public void nullExecutorTerminated() {
        ExecutorShutdown.Result result = ExecutorShutdown.shutdown(null, deadline(0));
        assertThat(result.terminated()).isTrue();
    }

    @Test
    @DisplayName("a wedged task is force-stopped and queued tasks are returned as dropped")
    public void wedgedTaskForcedAndDropped() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        CountDownLatch running = new CountDownLatch(1);
        pool.submit(() -> {
            running.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        AtomicInteger ranQueued = new AtomicInteger();
        pool.submit(ranQueued::incrementAndGet); // queued behind the wedged task
        running.await();

        ExecutorShutdown.Result result = ExecutorShutdown.shutdown(pool,
            deadline(TimeUnit.MILLISECONDS.toNanos(200)));
        // The graceful await consumes the budget, so shutdownNow fires and returns the queued task.
        assertThat(result.droppedTasks()).isNotEmpty();
        assertThat(ranQueued.get()).isZero();
        // The forced stop still interrupts the wedged task and the pool ends shortly after.
        assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
}
