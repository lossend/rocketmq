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
import java.util.function.BooleanSupplier;
import org.apache.rocketmq.proxy.lifecycle.FailureScope;
import org.apache.rocketmq.proxy.lifecycle.ReadinessContributor;
import org.apache.rocketmq.proxy.lifecycle.ReadinessResult;

/**
 * A {@link ReadinessContributor} backed by a synchronous {@link BooleanSupplier}.
 * Suitable for signals that are cheap and non-blocking to evaluate at check time,
 * such as "listener bound" or "processor started". A supplier that returns false or
 * throws yields a failure carrying this contributor's {@link FailureScope}.
 */
public final class SuppliedContributor implements ReadinessContributor {

    private final String name;
    private final FailureScope failureScope;
    private final BooleanSupplier signal;

    /**
     * Creates a supplier-backed contributor.
     *
     * @param name         stable contributor name used in logs and metrics
     * @param failureScope classification applied when the signal is not satisfied
     * @param signal       returns true when this readiness signal is satisfied
     */
    public SuppliedContributor(String name, FailureScope failureScope, BooleanSupplier signal) {
        this.name = name;
        this.failureScope = failureScope;
        this.signal = signal;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public CompletableFuture<ReadinessResult> check() {
        try {
            return CompletableFuture.completedFuture(signal.getAsBoolean()
                ? ReadinessResult.success()
                : ReadinessResult.failure(name + " not satisfied"));
        } catch (Exception e) {
            return CompletableFuture.completedFuture(ReadinessResult.failure(name + ": " + e.getMessage()));
        }
    }

    @Override
    public FailureScope failureScope() {
        return failureScope;
    }
}
