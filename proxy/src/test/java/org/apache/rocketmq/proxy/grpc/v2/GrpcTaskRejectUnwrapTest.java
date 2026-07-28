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

package org.apache.rocketmq.proxy.grpc.v2;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.future.FutureTaskExt;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks the behaviour that makes the reject path reachable at all: {@code submit} on
 * the pool type the proxy actually uses wraps the task, so a rejected runnable must
 * be unwrapped before it can be recognised.
 */
public class GrpcTaskRejectUnwrapTest {

    @Test
    @DisplayName("submit wraps the task, so a rejected runnable is a FutureTaskExt, not the original")
    public void submitWrapsTaskOnRejection() throws Exception {
        AtomicReference<Runnable> rejected = new AtomicReference<>();
        CountDownLatch release = new CountDownLatch(1);
        RejectedExecutionHandler handler = (r, executor) -> rejected.set(r);

        // Same executor type ThreadPoolMonitor.createAndMonitor builds.
        ExecutorService pool = ThreadUtils.newThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(1), r -> new Thread(r, "reject-unwrap-test"), handler);
        try {
            // Occupy the single worker, then fill the single queue slot.
            pool.submit(() -> awaitQuietly(release));
            pool.submit(() -> { });

            Runnable original = () -> { };
            pool.submit(original);

            assertThat(rejected.get()).isNotNull();
            // The core fact: the handler never sees the runnable that was submitted.
            assertThat(rejected.get()).isNotSameAs(original);
            assertThat(rejected.get()).isInstanceOf(FutureTaskExt.class);
            // Unwrapping recovers it, which is what castGrpcTask relies on.
            assertThat(((FutureTaskExt<?>) rejected.get()).getRunnable()).isSameAs(original);
        } finally {
            release.countDown();
            ((ThreadPoolExecutor) pool).shutdownNow();
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
