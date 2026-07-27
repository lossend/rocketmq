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
 * Outcome of a drain: either a clean DRAINED terminal or a forced drain. Never
 * describes resource-teardown failures; those belong to {@link StopResult}.
 */
public final class DrainResult {

    private final boolean forced;
    private final String reason;
    private final List<Throwable> causes;

    private DrainResult(boolean forced, String reason, List<Throwable> causes) {
        this.forced = forced;
        this.reason = reason;
        this.causes = causes == null ? Collections.emptyList() : Collections.unmodifiableList(causes);
    }

    public static DrainResult drained() {
        return new DrainResult(false, null, Collections.emptyList());
    }

    public static DrainResult forced(String reason, List<Throwable> causes) {
        return new DrainResult(true, reason, causes);
    }

    public boolean isForced() {
        return forced;
    }

    public String reason() {
        return reason;
    }

    public List<Throwable> causes() {
        return causes;
    }
}
