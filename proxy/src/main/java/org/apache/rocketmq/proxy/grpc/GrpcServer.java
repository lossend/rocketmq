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

package org.apache.rocketmq.proxy.grpc;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Server;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.lifecycle.ShutdownDeadline;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class GrpcServer implements StartAndShutdown,
    org.apache.rocketmq.proxy.lifecycle.grpc.GrpcDrainAdapter.PhasedGrpcServer {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final Server server;

    private final long timeout;

    private final TimeUnit unit;

    private final TlsCertificateManager tlsCertificateManager;
    @VisibleForTesting final GrpcTlsReloadHandler tlsReloadHandler;

    private final AtomicBoolean serverDrainStarted = new AtomicBoolean(false);
    private final AtomicBoolean forceStarted = new AtomicBoolean(false);

    protected GrpcServer(Server server, long timeout, TimeUnit unit,
        TlsCertificateManager tlsCertificateManager) throws Exception {
        this.server = server;
        this.timeout = timeout;
        this.unit = unit;
        this.tlsCertificateManager = tlsCertificateManager;
        this.tlsReloadHandler = new GrpcTlsReloadHandler();
    }

    public void start() throws Exception {
        // Register the TLS context reload handler
        tlsCertificateManager.registerReloadListener(this.tlsReloadHandler);

        this.server.start();
        log.info("grpc server start successfully.");
    }

    /**
     * Feature-off compatibility path. Builds one legacy deadline from the fixed
     * shutdown timeout and delegates to the phased lifecycle instead of the old
     * ignore-the-boolean chain, so it no longer swallows a non-terminated server.
     */
    public void shutdown() {
        ShutdownDeadline legacy = ShutdownDeadline.afterNanos(
            System.nanoTime(), unit.toNanos(timeout), System::nanoTime);
        try {
            initiateServerDrain();
            if (!awaitServerTermination(legacy)) {
                forceServerShutdown();
                awaitServerTermination(legacy);
            }
        } catch (InterruptedException e) {
            forceServerShutdown();
            Thread.currentThread().interrupt();
        } finally {
            unregisterTlsListener();
            log.info("grpc server shutdown finished.");
        }
    }

    /** Once-only, non-blocking ordered shutdown; triggers grpc-java's built-in double GOAWAY. */
    public void initiateServerDrain() {
        if (serverDrainStarted.compareAndSet(false, true)) {
            server.shutdown();
        }
    }

    /** Awaits termination within the deadline's remaining time; returns the raw boolean, never swallows interrupts. */
    public boolean awaitServerTermination(ShutdownDeadline deadline) throws InterruptedException {
        long remainingNanos = deadline.remainingNanos();
        return remainingNanos > 0
            ? server.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)
            : server.isTerminated();
    }

    /** Once-only forced cancellation of remaining streams. */
    public void forceServerShutdown() {
        if (forceStarted.compareAndSet(false, true)) {
            server.shutdownNow();
        }
    }

    /**
     * Bounded, deadline-aware teardown used by the strict lifecycle STOPPING phase.
     * Consumes only the remaining time of the shared {@link ShutdownDeadline}; if the
     * server does not terminate gracefully it is forced, then awaited once more within
     * whatever budget is left. A server that still refuses to terminate is logged as
     * {@code server_not_terminated} rather than blocking the hook. The TLS listener is
     * always unregistered.
     */
    public void shutdownOwnedResources(ShutdownDeadline stopDeadline) {
        try {
            initiateServerDrain();
            if (!awaitServerTermination(stopDeadline)) {
                forceServerShutdown();
                if (!awaitServerTermination(stopDeadline)) {
                    log.warn("grpc server_not_terminated within stop deadline");
                }
            }
        } catch (InterruptedException e) {
            forceServerShutdown();
            Thread.currentThread().interrupt();
        } finally {
            unregisterTlsListener();
        }
    }

    private void unregisterTlsListener() {
        tlsCertificateManager.unregisterReloadListener(this.tlsReloadHandler);
    }

    @VisibleForTesting
    class GrpcTlsReloadHandler implements TlsCertificateManager.TlsContextReloadListener {
        @Override
        public void onTlsContextReload() {
            try {
                ProxyAndTlsProtocolNegotiator.loadSslContext();
                log.info("SslContext reloaded for grpc server");
            } catch (CertificateException | IOException e) {
                log.error("Failed to reload SslContext for server", e);
            }
        }
    }
}
