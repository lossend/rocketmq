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
import org.junit.jupiter.api.DisplayName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConstructionScopeTest {

    @Test
    @DisplayName("close rolls back owned resources in reverse construction order")
    public void reverseOrderRollback() {
        List<String> closed = new ArrayList<>();
        try (ConstructionScope scope = new ConstructionScope()) {
            scope.own("a", () -> closed.add("a"), Runnable::run);
            scope.own("b", () -> closed.add("b"), Runnable::run);
            scope.own("c", () -> closed.add("c"), Runnable::run);
        }
        assertThat(closed).containsExactly("c", "b", "a");
    }

    @Test
    @DisplayName("commit transfers ownership so close is a no-op")
    public void commitSuppressesRollback() {
        List<String> closed = new ArrayList<>();
        try (ConstructionScope scope = new ConstructionScope()) {
            scope.own("a", () -> closed.add("a"), Runnable::run);
            scope.commit();
        }
        assertThat(closed).isEmpty();
    }

    @Test
    @DisplayName("a failure at the Nth leaf rolls back the earlier leaves in reverse order")
    public void nthLeafFailureRollsBackEarlier() {
        List<String> closed = new ArrayList<>();
        RuntimeException boom = new RuntimeException("leaf 3 failed");
        try {
            try (ConstructionScope scope = new ConstructionScope()) {
                scope.own("a", () -> closed.add("a"), Runnable::run);
                scope.own("b", () -> closed.add("b"), Runnable::run);
                throw boom; // simulate leaf 3 construction failing before own()
            }
        } catch (RuntimeException e) {
            assertThat(e).isSameAs(boom);
        }
        assertThat(closed).containsExactly("b", "a");
    }

    @Test
    @DisplayName("close failures are aggregated as suppressed exceptions and do not stop rollback")
    public void closeFailuresAggregated() {
        List<String> closed = new ArrayList<>();
        RuntimeException primary = new RuntimeException("primary");
        ConstructionScope scope = new ConstructionScope();
        scope.own("a", () -> closed.add("a"), Runnable::run);
        scope.own("bad", new Object(), r -> {
            throw new IllegalStateException("close failed");
        });
        scope.rollback(primary);
        // 'a' still closed despite the earlier close throwing
        assertThat(closed).containsExactly("a");
        assertThat(primary.getSuppressed()).hasSize(1);
        assertThat(primary.getSuppressed()[0]).isInstanceOf(IllegalStateException.class);
    }
}
