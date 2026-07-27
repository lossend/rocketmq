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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * Process-scoped owner of all proxy components. Replaces the former static
 * {@code ProxyStartAndShutdown} so a single instance owns an explicit ordered
 * component list and can be captured by the shutdown hook. Components start in
 * registration order and stop in reverse.
 * <p>
 * {@link #beginDrain(DrainTrigger)} and {@link #shutdown(DrainTrigger)} are two
 * distinct idempotent entry points: the former returns the same {@link DrainRun}
 * and stops at DRAINED/FORCE_DRAINING; the latter returns the same
 * {@link StopRun} stop future and uniquely drives STOPPING/STOPPED. Deadline-aware
 * bounded teardown is layered on in a later task; here the ownership, ordering,
 * and once-only stop future are established.
 */
public final class ProxyRuntime {

    private static final Logger log = LoggerFactory.getLogger("RocketmqProxy");

    private final List<StartAndShutdown> components;
    private final ProxyLifecycleCoordinator coordinator;
    private final boolean lifecycleEnabled;

    private final AtomicReference<StopRun> stopRunRef = new AtomicReference<>();

    public ProxyRuntime(List<StartAndShutdown> components, ProxyLifecycleCoordinator coordinator,
        boolean lifecycleEnabled) {
        this.components = new ArrayList<>(components);
        this.coordinator = coordinator;
        this.lifecycleEnabled = lifecycleEnabled;
    }

    public ProxyLifecycleCoordinator coordinator() {
        return coordinator;
    }

    public void start() throws Exception {
        for (StartAndShutdown component : components) {
            component.start();
        }
        if (coordinator != null) {
            coordinator.markStarted();
            coordinator.markReady();
        }
    }

    /** Idempotent drain entry: returns the single {@link DrainRun}, stopping at DRAINED/FORCE_DRAINING. */
    public DrainRun beginDrain(DrainTrigger trigger) {
        if (coordinator == null) {
            throw new IllegalStateException("lifecycle coordinator is not enabled");
        }
        return coordinator.beginDrain(trigger);
    }

    /**
     * Idempotent stop entry. The first caller publishes the single {@link StopRun}
     * and runs the reverse-order shutdown; later callers get the same stop future.
     */
    public CompletableFuture<StopResult> shutdown(DrainTrigger trigger) {
        StopRun existing = stopRunRef.get();
        if (existing != null) {
            return existing.stopFuture();
        }
        CompletableFuture<StopResult> future = new CompletableFuture<>();
        StopRun candidate = new StopRun(null, future);
        if (!stopRunRef.compareAndSet(null, candidate)) {
            return stopRunRef.get().stopFuture();
        }
        List<Throwable> causes = new ArrayList<>();
        for (int i = components.size() - 1; i >= 0; i--) {
            StartAndShutdown component = components.get(i);
            try {
                component.shutdown();
            } catch (Throwable t) {
                log.error("error shutting down component {}", component, t);
                causes.add(t);
            }
        }
        future.complete(causes.isEmpty() ? StopResult.clean() : StopResult.withCauses(causes));
        return future;
    }

    /** Startup-failure stop path; shares the stop CAS but never reports a clean DRAINED. */
    public CompletableFuture<StopResult> forceStop(Throwable startupFailure) {
        log.error("forcing proxy runtime stop after startup failure", startupFailure);
        return shutdown(DrainTrigger.SIGTERM_FALLBACK);
    }

    public boolean isLifecycleEnabled() {
        return lifecycleEnabled;
    }
}
