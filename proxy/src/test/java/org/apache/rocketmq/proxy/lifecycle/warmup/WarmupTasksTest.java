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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;

public class WarmupTasksTest {

    @Test
    @DisplayName("supplied task reflects its boolean signal and scope")
    public void suppliedReflectsSignal() {
        WarmupTask up = WarmupTasks.supplied("listener", 10, FailureScope.LOCAL_FATAL, () -> true);
        WarmupTask down = WarmupTasks.supplied("listener", 10, FailureScope.LOCAL_FATAL, () -> false);

        assertThat(up.warmup().join().isSuccess()).isTrue();
        assertThat(down.warmup().join().isSuccess()).isFalse();
        assertThat(up.failureScope()).isEqualTo(FailureScope.LOCAL_FATAL);
        assertThat(up.priority()).isEqualTo(10);
    }

    @Test
    @DisplayName("nameServerReachable is SHARED_DEPENDENCY and reflects the probe")
    public void nameServerReflectsProbe() {
        WarmupTask reachable = WarmupTasks.nameServerReachable(40, () -> true);
        WarmupTask unreachable = WarmupTasks.nameServerReachable(40, () -> false);

        assertThat(reachable.warmup().join().isSuccess()).isTrue();
        assertThat(unreachable.warmup().join().isSuccess()).isFalse();
        assertThat(reachable.failureScope()).isEqualTo(FailureScope.SHARED_DEPENDENCY);
    }

    @Test
    @DisplayName("a probe that throws is caught and reported as failure, not propagated")
    public void probeExceptionCaught() {
        WarmupTask task = WarmupTasks.nameServerReachable(40, () -> {
            throw new IllegalStateException("boom");
        });

        WarmupResult result = task.warmup().join();

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.detail()).contains("boom");
    }

    @Test
    @DisplayName("brokerReachable passes on the first reachable broker for the topic")
    public void brokerReachablePassesOnFirstReachable() {
        Function<String, List<String>> two = topic -> Arrays.asList("a:10911", "b:10911");
        Predicate<String> onlyB = addr -> addr.startsWith("b");

        WarmupTask task = WarmupTasks.brokerReachable("TopicX", 50, two, onlyB);

        assertThat(task.warmup().join().isSuccess()).isTrue();
        assertThat(task.name()).contains("TopicX");
    }

    @Test
    @DisplayName("brokerReachable fails when the topic route resolves to no broker")
    public void brokerReachableFailsOnEmptyRoute() {
        WarmupTask task = WarmupTasks.brokerReachable("TopicX", 50,
            topic -> Collections.emptyList(), addr -> true);

        assertThat(task.warmup().join().isSuccess()).isFalse();
    }
}
