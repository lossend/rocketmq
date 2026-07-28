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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Explicit registry of {@link WarmupTask}s gating the STARTING to READY transition.
 * Modules register their warmup steps here instead of the driver hardcoding them.
 *
 * <p>Registration is de-duplicated by {@link WarmupTask#name()} — a duplicate name is a
 * programming error and fails fast. {@link #tasks()} returns a defensive copy sorted by
 * {@link WarmupTask#priority()} ascending, with a stable tie-break preserving registration
 * order. Assembly runs on the single startup thread, so no extra synchronization is needed.
 */
public final class WarmupRegistry {

    private final List<WarmupTask> registered = new ArrayList<>();
    private final Set<String> names = new HashSet<>();

    /**
     * Registers a warmup task.
     *
     * @param task the task to register
     * @throws IllegalStateException if a task with the same {@link WarmupTask#name()} is
     *                               already registered
     */
    public void register(WarmupTask task) {
        if (!names.add(task.name())) {
            throw new IllegalStateException("duplicate warmup task name: " + task.name());
        }
        registered.add(task);
    }

    /**
     * Returns the registered tasks in evaluation order: ascending priority, ties broken by
     * registration order.
     *
     * @return a defensive, priority-sorted copy of the registered tasks
     */
    public List<WarmupTask> tasks() {
        List<WarmupTask> ordered = new ArrayList<>(registered);
        ordered.sort(Comparator.comparingInt(WarmupTask::priority));
        return ordered;
    }
}
