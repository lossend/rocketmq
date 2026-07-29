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

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.apache.rocketmq.proxy.lifecycle.ReadinessResult;

/**
 * One registrable runtime health check backing {@code /ready-for-traffic}. Unlike a
 * {@code WarmupTask}, a {@code ReadinessProbe} is re-evaluated on every readiness query
 * after startup warmup has latched — it is a live dependency health signal, not a
 * one-time startup gate.
 *
 * <p>A module contributes a probe by registering it into a {@link ReadinessProbeRegistry}.
 * The {@link FailureScope} decides how a failure affects the fail-close threshold in
 * {@code ReadinessEvaluator#isReadyForTraffic()}: {@code LOCAL_FATAL} fails immediately,
 * {@code SHARED_DEPENDENCY} counts toward the consecutive-failure threshold.
 */
public interface ReadinessProbe {

    /**
     * Stable, unique probe name. Registration rejects a duplicate name.
     *
     * @return the probe name used for de-dup and logging
     */
    String name();

    /**
     * Evaluates this probe. Must be non-blocking or client-side time-bounded, since it is
     * called synchronously on every readiness query.
     *
     * @return a future completing with success or failure
     */
    CompletableFuture<ReadinessResult> check();

    /**
     * Classifies how a failure of this probe affects {@code /ready-for-traffic}.
     *
     * @return the failure scope
     */
    FailureScope failureScope();
}
