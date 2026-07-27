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

import java.util.concurrent.CompletableFuture;

/**
 * An immutable pairing of the shared stop deadline and the once-only stop
 * future, published together so all nested owners observe the same deadline.
 */
public final class StopRun {

    private final ShutdownDeadline stopDeadline;
    private final CompletableFuture<StopResult> stopFuture;

    public StopRun(ShutdownDeadline stopDeadline, CompletableFuture<StopResult> stopFuture) {
        this.stopDeadline = stopDeadline;
        this.stopFuture = stopFuture;
    }

    public ShutdownDeadline stopDeadline() {
        return stopDeadline;
    }

    public CompletableFuture<StopResult> stopFuture() {
        return stopFuture;
    }
}
