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
 * Request-scoped view of a single accepted send's dual-terminal lifecycle. The
 * backend (Broker future) and protocol (response write) terminals are tracked
 * independently; the permit is only released once both are terminal.
 */
public interface SendLifecycleContext {

    /** Only the NOT_STARTED -&gt; STARTED transition returns true; the caller may then dispatch to the Broker. */
    boolean backendStarted();

    /** First STARTED -&gt; TERMINAL wins; TERMINAL re-entry is an idempotent no-op. */
    void backendTerminal(Throwable cause);

    /** Only NOT_STARTED -&gt; SKIPPED returns true. Losing the race is a normal cancel/dispatch outcome, not a failure. */
    boolean tryBackendSkipped(SkipReason reason);

    /** Records the protocol (response-write) terminal. */
    void protocolTerminal(ProtocolResult result);

    /** Completes exactly once when the permit is RELEASED (both terminals reached). */
    CompletableFuture<Void> completionFuture();
}
