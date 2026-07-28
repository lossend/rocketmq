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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.lifecycle.warmup.WarmupRegistry;
import org.apache.rocketmq.proxy.lifecycle.warmup.WarmupTask;
import org.apache.rocketmq.proxy.lifecycle.warmup.WarmupTaskContributorAdapter;
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

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    // No dedicated config yet; a small default tolerates transient shared-dependency
    // blips for /ready-for-traffic without holding warmup open indefinitely.
    private static final int DEFAULT_DEPENDENCY_FAILURE_THRESHOLD = 3;
    // Fixed retry cadence for the warmup barrier while it has not yet completed.
    private static final long WARMUP_RETRY_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

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

    // Captured at createCoordinator so startWarmup can drive the barrier on the same
    // single-threaded lifecycle scheduler used for drain timers.
    private volatile LifecycleScheduler scheduler;

    public ProxyGracefulLifecycleWiring() {
        this.tracerFactory = new GrpcSendStreamTracerFactory(openSendRpcs);
        this.sendInterceptor = new GrpcSendLifecycleInterceptor(gate);
        this.activeCallInterceptor = new GrpcActiveCallInterceptor(activeCallRegistry, drainStatusPolicy);
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
        return createCoordinator(grpcServer, config, scheduler, ForkJoinPool.commonPool());
    }

    /**
     * Builds the coordinator with an explicitly owned executor for the blocking
     * gRPC server-termination wait.
     */
    public ProxyLifecycleCoordinator createCoordinator(GrpcDrainAdapter.PhasedGrpcServer grpcServer,
        ProxyConfig config, LifecycleScheduler scheduler, Executor terminationExecutor) {
        this.scheduler = scheduler;
        GrpcDrainAdapter adapter = new GrpcDrainAdapter(grpcServer, activeCallRegistry, drainStatusPolicy,
            terminationExecutor);
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
        // Diagnostic lb-detach quiet observation: reports whether new business
        // connections were still arriving near the cutoff, so the configured
        // proxyLbDetachTimeoutSeconds can be calibrated against real provider timing.
        coordinator.observeLbDetachQuiet(transportFilter::quietDurationNanos,
            config.getProxyLbDetachQuietSeconds(), metrics);
        coordinatorRef.set(coordinator);
        return coordinator;
    }

    /**
     * Installs a warmup barrier over the given contributors and drives it on the
     * lifecycle scheduler until every contributor passes once, at which point
     * {@code onReady} runs (publishing READY). The barrier also backs the
     * coordinator's {@code isReadyForTraffic()} predicate.
     *
     * <p>The scheduler exposes only one-shot delays, so each tick re-schedules the
     * next while the barrier has not completed. On warmup-timeout the loop keeps
     * retrying (the pod stays STARTING/NotReady) and only escalates the log level;
     * it never exits the process or marks fatal. K8s rollout timeout is the outer
     * guard.
     *
     * @param contributors readiness contributors evaluated as a one-time barrier
     * @param config       supplies the warmup timeout budget
     * @param scheduler    lifecycle scheduler driving the retry loop
     * @param onReady      published exactly once when the barrier completes
     */
    public void startWarmup(List<ReadinessContributor> contributors, ProxyConfig config, Runnable onReady) {
        LifecycleScheduler active = scheduler;
        if (active == null) {
            throw new IllegalStateException("startWarmup requires createCoordinator to run first");
        }
        startWarmup(contributors, config, active, onReady);
    }

    /**
     * Drives warmup from a {@link WarmupRegistry}: registered tasks are adapted to readiness
     * contributors (in registry priority order) and evaluated by the same barrier. Uses the
     * scheduler captured at {@code createCoordinator}.
     *
     * @param registry the registered warmup tasks
     * @param config   supplies the warmup timeout budget
     * @param onReady  published exactly once when the barrier completes
     */
    public void startWarmup(WarmupRegistry registry, ProxyConfig config, Runnable onReady) {
        startWarmup(adapt(registry), config, onReady);
    }

    /**
     * Registry-driven warmup with an explicit scheduler; primarily for tests.
     *
     * @param registry  the registered warmup tasks
     * @param config    supplies the warmup timeout budget
     * @param scheduler lifecycle scheduler driving the retry loop
     * @param onReady   published exactly once when the barrier completes
     */
    public void startWarmup(WarmupRegistry registry, ProxyConfig config,
        LifecycleScheduler scheduler, Runnable onReady) {
        startWarmup(adapt(registry), config, scheduler, onReady);
    }

    private static List<ReadinessContributor> adapt(WarmupRegistry registry) {
        List<ReadinessContributor> contributors = new ArrayList<>();
        for (WarmupTask task : registry.tasks()) {
            contributors.add(new WarmupTaskContributorAdapter(task));
        }
        return contributors;
    }

    /**
     * Same as {@link #startWarmup(List, ProxyConfig, Runnable)} but with an explicit
     * scheduler; primarily for tests that drive the loop synchronously.
     *
     * @param contributors readiness contributors evaluated as a one-time barrier
     * @param config       supplies the warmup timeout budget
     * @param scheduler    lifecycle scheduler driving the retry loop
     * @param onReady      published exactly once when the barrier completes
     */
    public void startWarmup(List<ReadinessContributor> contributors, ProxyConfig config,
        LifecycleScheduler scheduler, Runnable onReady) {
        ReadinessBarrier barrier = new ReadinessBarrier(contributors, DEFAULT_DEPENDENCY_FAILURE_THRESHOLD);
        ProxyLifecycleCoordinator coordinator = coordinatorRef.get();
        if (coordinator != null) {
            coordinator.setReadinessBarrier(barrier);
        }
        long deadlineNanos = System.nanoTime()
            + TimeUnit.SECONDS.toNanos(config.getProxyWarmupTimeoutSeconds());
        driveWarmupTick(barrier, scheduler, onReady, System::nanoTime, deadlineNanos);
    }

    private void driveWarmupTick(ReadinessBarrier barrier, LifecycleScheduler scheduler,
        Runnable onReady, LongSupplier nanoClock, long deadlineNanos) {
        boolean completed;
        try {
            completed = barrier.tryCompleteWarmup();
        } catch (Exception e) {
            completed = false;
            log.warn("warmup barrier evaluation threw; will retry", e);
        }
        if (completed) {
            log.info("warmup barrier completed; publishing readiness");
            onReady.run();
            return;
        }
        if (nanoClock.getAsLong() - deadlineNanos >= 0L) {
            log.warn("warmup barrier still incomplete past proxyWarmupTimeoutSeconds; "
                + "staying NotReady and retrying");
        } else {
            log.info("warmup barrier incomplete; retrying");
        }
        scheduler.schedule(() -> driveWarmupTick(barrier, scheduler, onReady, nanoClock, deadlineNanos),
            WARMUP_RETRY_INTERVAL_NANOS);
    }
}
