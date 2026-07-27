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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.lifecycle.grpc.GrpcActiveCallInterceptor;
import org.apache.rocketmq.proxy.lifecycle.grpc.GrpcActiveCallRegistry;
import org.apache.rocketmq.proxy.lifecycle.grpc.GrpcDrainAdapter;
import org.apache.rocketmq.proxy.lifecycle.grpc.GrpcDrainStatusPolicy;
import org.apache.rocketmq.proxy.lifecycle.grpc.GrpcSendLifecycleInterceptor;
import org.apache.rocketmq.proxy.lifecycle.grpc.GrpcSendStreamTracerFactory;
import org.apache.rocketmq.proxy.lifecycle.grpc.GrpcTransportLifecycleFilter;

/**
 * Assembles the gRPC graceful-lifecycle wiring so {@code ProxyStartup} stays
 * small. The gRPC interceptors/tracer/filter are built up front (the builder
 * needs them), while the coordinator is created after the {@code GrpcServer}
 * exists. The transport filter reads the active lb cutoff through a late-bound
 * coordinator reference so a late transport can be flagged once a drain begins.
 */
public final class ProxyGracefulLifecycleWiring {

    private final SendDrainGate gate = new SendDrainGate();
    private final ProxyLifecycleMetrics metrics = new ProxyLifecycleMetrics();
    private final AtomicLong openSendRpcs = new AtomicLong();
    private final GrpcActiveCallRegistry activeCallRegistry = new GrpcActiveCallRegistry();
    private final GrpcDrainStatusPolicy drainStatusPolicy = new GrpcDrainStatusPolicy();
    private final AtomicReference<ProxyLifecycleCoordinator> coordinatorRef = new AtomicReference<>();

    private final GrpcSendStreamTracerFactory tracerFactory;
    private final GrpcSendLifecycleInterceptor sendInterceptor;
    private final GrpcActiveCallInterceptor activeCallInterceptor;
    private final GrpcTransportLifecycleFilter transportFilter;

    public ProxyGracefulLifecycleWiring() {
        this.tracerFactory = new GrpcSendStreamTracerFactory(openSendRpcs);
        this.sendInterceptor = new GrpcSendLifecycleInterceptor(gate);
        this.activeCallInterceptor = new GrpcActiveCallInterceptor(activeCallRegistry);
        this.transportFilter = new GrpcTransportLifecycleFilter(System::nanoTime, this::activeLbCutoffNanos);
    }

    private long activeLbCutoffNanos() {
        ProxyLifecycleCoordinator coordinator = coordinatorRef.get();
        return coordinator == null ? 0L : coordinator.activeLbCutoffNanos();
    }

    public GrpcSendStreamTracerFactory tracerFactory() {
        return tracerFactory;
    }

    public GrpcSendLifecycleInterceptor sendInterceptor() {
        return sendInterceptor;
    }

    public GrpcActiveCallInterceptor activeCallInterceptor() {
        return activeCallInterceptor;
    }

    public GrpcTransportLifecycleFilter transportFilter() {
        return transportFilter;
    }

    public ProxyLifecycleMetrics metrics() {
        return metrics;
    }

    public ProxyLifecycleCoordinator coordinator() {
        return coordinatorRef.get();
    }

    /**
     * Builds the coordinator once the phased gRPC server exists, publishes it for
     * the transport filter's lb-cutoff supplier, and returns it.
     */
    public ProxyLifecycleCoordinator createCoordinator(GrpcDrainAdapter.PhasedGrpcServer grpcServer,
        ProxyConfig config, LifecycleScheduler scheduler) {
        GrpcDrainAdapter adapter = new GrpcDrainAdapter(grpcServer, activeCallRegistry, drainStatusPolicy);
        ProxyLifecycleCoordinator coordinator = new ProxyLifecycleCoordinator(
            gate,
            Collections.singletonList(adapter),
            scheduler,
            System::nanoTime,
            true,
            null,
            config.getProxyLbDetachTimeoutSeconds(),
            config.getProxyConnectionLeaseSeconds(),
            config.getProxyRemotingLeaseJitterRatio(),
            config.getProxyConnectionLeaseGraceSeconds(),
            config.getProxySendDrainTimeoutSeconds(),
            config.getProxyPreStopWaitSeconds());
        coordinatorRef.set(coordinator);
        return coordinator;
    }
}
