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

/**
 * Small scheduling seam the coordinator uses instead of touching a
 * {@code ScheduledExecutorService} directly. Kept minimal so tests can drive
 * phase timers deterministically with a fake, without real sleeps.
 */
public interface LifecycleScheduler {

    /** Run once, off the calling thread, as soon as possible. */
    void execute(Runnable task);

    /** Schedule a one-shot task after a delay; the handle can cancel it before it fires. */
    ScheduledHandle schedule(Runnable task, long delayNanos);

    interface ScheduledHandle {
        boolean cancel();
    }
}
