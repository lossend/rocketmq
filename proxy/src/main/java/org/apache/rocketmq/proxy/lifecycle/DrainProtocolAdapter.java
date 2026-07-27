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
 * Per-protocol drain seam the coordinator fans out to. The four methods map to
 * the single closing order: startMigration = migration_started; the coordinator
 * closes the shared gate once every adapter reports noNewWorkReached; then it
 * waits for accepted sends to drain before awaitTerminated. force() is a bounded
 * teardown driven by the effective deadline.
 */
public interface DrainProtocolAdapter {

    SendProtocol protocol();

    /** Begin migrating existing connections; non-blocking. Marks migration_started. */
    void startMigration();

    /** Completes once this protocol will not dispatch new business work (no_new_work_reached). */
    CompletableFuture<Void> noNewWorkReached();

    /** Await this protocol's transport termination within the effective deadline. */
    CompletableFuture<Void> awaitTerminated(ShutdownDeadline effectiveDeadline);

    /** Bounded, idempotent forced teardown driven by the effective deadline. */
    void force(ShutdownDeadline effectiveDeadline);
}
