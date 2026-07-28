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

import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.apache.rocketmq.proxy.lifecycle.ReadinessResult;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;

public class NameServerReachableContributorTest {

    @Test
    @DisplayName("a reachable NameServer probe succeeds and is classified SHARED_DEPENDENCY")
    public void reachableProbeSucceeds() {
        NameServerReachableContributor contributor = new NameServerReachableContributor(() -> true);

        ReadinessResult result = contributor.check().join();

        assertThat(result.isSuccess()).isTrue();
        assertThat(contributor.failureScope()).isEqualTo(FailureScope.SHARED_DEPENDENCY);
    }

    @Test
    @DisplayName("an unreachable NameServer probe fails without recovering")
    public void unreachableProbeFails() {
        NameServerReachableContributor contributor = new NameServerReachableContributor(() -> false);

        ReadinessResult result = contributor.check().join();

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.detail()).contains("unreachable");
    }

    @Test
    @DisplayName("a probe that throws is reported as a failure, not propagated")
    public void throwingProbeIsCaught() {
        NameServerReachableContributor contributor = new NameServerReachableContributor(() -> {
            throw new IllegalStateException("connection refused");
        });

        ReadinessResult result = contributor.check().join();

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.detail()).contains("connection refused");
    }
}
