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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Explicit registry of {@link ReadinessProbe}s backing {@code /ready-for-traffic}. Modules
 * register their runtime health checks here instead of the evaluator hardcoding them.
 *
 * <p>Registration is de-duplicated by {@link ReadinessProbe#name()} — a duplicate name is a
 * programming error and fails fast. {@link #probes()} returns a defensive copy; evaluation
 * order does not matter since every probe contributes independently to the fail-close
 * threshold. Assembly runs on the single startup thread, so no extra synchronization is
 * needed.
 */
public final class ReadinessProbeRegistry {

    private final List<ReadinessProbe> registered = new ArrayList<>();
    private final Set<String> names = new HashSet<>();

    /**
     * Registers a readiness probe.
     *
     * @param probe the probe to register
     * @throws IllegalStateException if a probe with the same {@link ReadinessProbe#name()} is
     *                                already registered
     */
    public void register(ReadinessProbe probe) {
        if (!names.add(probe.name())) {
            throw new IllegalStateException("duplicate readiness probe name: " + probe.name());
        }
        registered.add(probe);
    }

    /**
     * Returns the registered probes.
     *
     * @return a defensive copy of the registered probes
     */
    public List<ReadinessProbe> probes() {
        return new ArrayList<>(registered);
    }
}
