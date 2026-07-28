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

package org.apache.rocketmq.proxy.lifecycle.grpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.proxy.lifecycle.DrainProtocolAdapter;
import org.apache.rocketmq.proxy.lifecycle.SendProtocol;
import org.apache.rocketmq.proxy.lifecycle.ShutdownDeadline;

/**
 * Binds the gRPC server and active-call registry to the coordinator's
 * {@link DrainProtocolAdapter} seam. Per the single closing order:
 * startMigration initiates the built-in double GOAWAY (migration_started);
 * noNewWorkReached completes once GOAWAY has been issued; the coordinator then
 * closes the shared gate; awaitTerminated closes the non-unary registry, waits
 * for its drain, and awaits server termination within the effective deadline.
 * force() is a once-only bounded teardown.
 */
public final class GrpcDrainAdapter implements DrainProtocolAdapter {

    /** Minimal view of the phased gRPC server so the adapter is testable with a fake. */
    public interface PhasedGrpcServer {
        void initiateServerDrain();

        boolean awaitServerTermination(ShutdownDeadline deadline) throws InterruptedException;

        void forceServerShutdown();
    }

    private final PhasedGrpcServer server;
    private final GrpcActiveCallRegistry registry;
    private final GrpcDrainStatusPolicy policy;
    private final Executor terminationExecutor;

    private final AtomicBoolean migrationStarted = new AtomicBoolean(false);
    private final AtomicBoolean forced = new AtomicBoolean(false);
    private final CompletableFuture<Void> noNewWork = new CompletableFuture<>();

    /**
     * @deprecated supply a managed termination executor so blocking server waits
     *             have an explicit owner.
     */
    @Deprecated
    public GrpcDrainAdapter(PhasedGrpcServer server, GrpcActiveCallRegistry registry,
        GrpcDrainStatusPolicy policy) {
        this(server, registry, policy, ForkJoinPool.commonPool());
    }

    public GrpcDrainAdapter(PhasedGrpcServer server, GrpcActiveCallRegistry registry,
        GrpcDrainStatusPolicy policy, Executor terminationExecutor) {
        this.server = server;
        this.registry = registry;
        this.policy = policy;
        this.terminationExecutor = terminationExecutor;
    }

    @Override
    public SendProtocol protocol() {
        return SendProtocol.GRPC;
    }

    @Override
    public void startMigration() {
        if (migrationStarted.compareAndSet(false, true)) {
            server.initiateServerDrain();
            // The built-in double GOAWAY is the no-new-work boundary for the primary spike outcome.
            noNewWork.complete(null);
        }
    }

    @Override
    public CompletableFuture<Void> noNewWorkReached() {
        return noNewWork;
    }

    @Override
    public CompletableFuture<Void> awaitTerminated(ShutdownDeadline effectiveDeadline) {
        registry.closeAll(policy);
        return registry.drainedFuture().thenComposeAsync(ignored -> {
            CompletableFuture<Void> done = new CompletableFuture<>();
            try {
                if (server.awaitServerTermination(effectiveDeadline)) {
                    done.complete(null);
                } else {
                    server.forceServerShutdown();
                    done.completeExceptionally(new IllegalStateException("server_not_terminated"));
                }
            } catch (InterruptedException e) {
                server.forceServerShutdown();
                done.completeExceptionally(e);
                Thread.currentThread().interrupt();
            }
            return done;
        }, terminationExecutor);
    }

    @Override
    public void force(ShutdownDeadline effectiveDeadline) {
        if (forced.compareAndSet(false, true)) {
            registry.closeAll(policy);
            server.forceServerShutdown();
        }
    }
}
