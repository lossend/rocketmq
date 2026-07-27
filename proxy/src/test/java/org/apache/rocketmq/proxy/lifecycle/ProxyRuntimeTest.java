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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProxyRuntimeTest {

    private static final class RecordingComponent implements StartAndShutdown {
        private final String name;
        private final List<String> log;
        private final boolean failShutdown;

        RecordingComponent(String name, List<String> log, boolean failShutdown) {
            this.name = name;
            this.log = log;
            this.failShutdown = failShutdown;
        }

        @Override
        public void start() {
            log.add("start:" + name);
        }

        @Override
        public void shutdown() {
            log.add("shutdown:" + name);
            if (failShutdown) {
                throw new RuntimeException("shutdown failed: " + name);
            }
        }
    }

    @Test
    @DisplayName("components start in registration order and stop in reverse")
    public void startForwardStopReverse() throws Exception {
        List<String> log = new ArrayList<>();
        List<StartAndShutdown> components = Arrays.asList(
            new RecordingComponent("a", log, false),
            new RecordingComponent("b", log, false),
            new RecordingComponent("c", log, false));
        ProxyRuntime runtime = new ProxyRuntime(components, null, false);

        runtime.start();
        runtime.shutdown(DrainTrigger.SIGTERM_FALLBACK).join();

        assertThat(log).containsExactly(
            "start:a", "start:b", "start:c",
            "shutdown:c", "shutdown:b", "shutdown:a");
    }

    @Test
    @DisplayName("repeated shutdown returns the same stop future and stops only once")
    public void shutdownIsIdempotent() {
        List<String> log = new ArrayList<>();
        List<StartAndShutdown> components = Arrays.asList(
            new RecordingComponent("a", log, false));
        ProxyRuntime runtime = new ProxyRuntime(components, null, false);

        CompletableFuture<StopResult> first = runtime.shutdown(DrainTrigger.PRESTOP);
        CompletableFuture<StopResult> second = runtime.shutdown(DrainTrigger.SIGTERM_FALLBACK);

        assertThat(second).isSameAs(first);
        assertThat(log).containsExactly("shutdown:a");
    }

    @Test
    @DisplayName("a shutdown failure is collected but does not stop the remaining components")
    public void shutdownFailureContinues() {
        List<String> log = new ArrayList<>();
        List<StartAndShutdown> components = Arrays.asList(
            new RecordingComponent("a", log, false),
            new RecordingComponent("b", log, true),
            new RecordingComponent("c", log, false));
        ProxyRuntime runtime = new ProxyRuntime(components, null, false);

        StopResult result = runtime.shutdown(DrainTrigger.PRESTOP).join();

        assertThat(log).containsExactly("shutdown:c", "shutdown:b", "shutdown:a");
        assertThat(result.isClean()).isFalse();
        assertThat(result.causes()).hasSize(1);
    }

    @Test
    @DisplayName("beginDrain without a coordinator is rejected")
    public void beginDrainRequiresCoordinator() {
        ProxyRuntime runtime = new ProxyRuntime(new ArrayList<>(), null, false);
        try {
            runtime.beginDrain(DrainTrigger.PRESTOP);
            org.junit.Assert.fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            assertThat(expected).hasMessageContaining("coordinator");
        }
    }
}
