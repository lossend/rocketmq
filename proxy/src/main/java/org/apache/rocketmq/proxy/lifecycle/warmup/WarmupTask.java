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

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;

/**
 * One registrable warmup step gating the STARTING to READY transition. A module
 * contributes a warmup step by registering a {@code WarmupTask} into the
 * {@link WarmupRegistry}; the warmup driver evaluates every registered task as a
 * one-time barrier before publishing readiness.
 *
 * <p>Isomorphic to the lifecycle readiness contributor so it adapts to the existing
 * barrier without changing its fail-open/fail-close semantics. The {@link FailureScope}
 * carried here decides, after the first READY, whether a later failure fails
 * {@code /ready} (LOCAL_FATAL) or only {@code /ready-for-traffic} (SHARED_DEPENDENCY).
 */
public interface WarmupTask {

    /**
     * Stable, unique task name. Registration rejects a duplicate name.
     *
     * @return the task name used for de-dup, ordering tie-break, and logging
     */
    String name();

    /**
     * Ordering key: tasks run in ascending priority, ties broken by registration order.
     *
     * @return the priority; lower runs first
     */
    int priority();

    /**
     * Evaluates this warmup step once. Must be non-blocking or client-side time-bounded
     * so it cannot wedge the single-threaded warmup scheduler.
     *
     * @return a future completing with success or failure
     */
    CompletableFuture<WarmupResult> warmup();

    /**
     * Classifies how a failure of this task affects the readiness predicates after warmup.
     *
     * @return the failure scope
     */
    FailureScope failureScope();
}
