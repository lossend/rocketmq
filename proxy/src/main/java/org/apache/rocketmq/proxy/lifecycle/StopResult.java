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

import java.util.Collections;
import java.util.List;

/**
 * Outcome of a process stop: describes resource-teardown exceptions/timeouts,
 * never a drain outcome. A single logging or close failure must not stall the
 * transition to STOPPED, so causes accumulate rather than short-circuit.
 */
public final class StopResult {

    private final List<Throwable> causes;

    private StopResult(List<Throwable> causes) {
        this.causes = causes == null ? Collections.emptyList() : Collections.unmodifiableList(causes);
    }

    public static StopResult clean() {
        return new StopResult(Collections.emptyList());
    }

    public static StopResult withCauses(List<Throwable> causes) {
        return new StopResult(causes);
    }

    public boolean isClean() {
        return causes.isEmpty();
    }

    public List<Throwable> causes() {
        return causes;
    }
}
