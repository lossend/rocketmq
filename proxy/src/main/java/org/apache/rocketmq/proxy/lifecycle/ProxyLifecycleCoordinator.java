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

import java.time.Instant;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Single-owner lifecycle coordinator. State lives in one {@link AtomicReference};
 * the active drain lives in one {@link AtomicReference} published as a whole
 * {@link DrainRun} so readers never see a torn session/future pair. The DRAINING
 * segment follows the single closing order: startMigration (migration_started)
 * -&gt; noNewWorkReached -&gt; closeAdmission (admission_closed, once) -&gt;
 * accepted-inflight zero -&gt; awaitTerminated. Only one drain runs; repeated
 * triggers reuse the winning {@link DrainRun}.
 */
public final class ProxyLifecycleCoordinator implements ProxyLifecycle {

    private static final Map<ProxyLifecycleState, EnumSet<ProxyLifecycleState>> LEGAL =
        legalTransitions();

    private final SendDrainGate gate;
    private final List<DrainProtocolAdapter> adapters;
    private final LifecycleScheduler scheduler;
    private final LongSupplier nanoClock;
    private final boolean lifecycleEnabled;
    private final Runnable readinessWithdraw;

    // Drain session budgets (seconds), captured at construction.
    private final int lbDetachTimeoutSeconds;
    private final int connectionLeaseSeconds;
    private final double remotingLeaseJitterRatio;
    private final int connectionLeaseGraceSeconds;
    private final int sendDrainTimeoutSeconds;
    private final int preStopWaitSeconds;

    private final AtomicReference<ProxyLifecycleState> state =
        new AtomicReference<>(ProxyLifecycleState.STARTING);
    private final AtomicReference<DrainRun> drainRunRef = new AtomicReference<>();
    private final AtomicReference<ShutdownDeadline> stopOverride = new AtomicReference<>();
    private final AtomicInteger phaseGeneration = new AtomicInteger();
    private final AtomicReference<String> reasonRef = new AtomicReference<>("");
    private final AtomicReference<Throwable> fatalRef = new AtomicReference<>();

    private volatile LifecycleScheduler.ScheduledHandle lbTimer;
    private volatile LifecycleScheduler.ScheduledHandle hardDeadlineTimer;

    public ProxyLifecycleCoordinator(SendDrainGate gate, List<DrainProtocolAdapter> adapters,
        LifecycleScheduler scheduler, LongSupplier nanoClock, boolean lifecycleEnabled,
        Runnable readinessWithdraw, int lbDetachTimeoutSeconds, int connectionLeaseSeconds,
        double remotingLeaseJitterRatio, int connectionLeaseGraceSeconds,
        int sendDrainTimeoutSeconds, int preStopWaitSeconds) {
        this.gate = gate;
        this.adapters = adapters;
        this.scheduler = scheduler;
        this.nanoClock = nanoClock;
        this.lifecycleEnabled = lifecycleEnabled;
        this.readinessWithdraw = readinessWithdraw;
        this.lbDetachTimeoutSeconds = lbDetachTimeoutSeconds;
        this.connectionLeaseSeconds = connectionLeaseSeconds;
        this.remotingLeaseJitterRatio = remotingLeaseJitterRatio;
        this.connectionLeaseGraceSeconds = connectionLeaseGraceSeconds;
        this.sendDrainTimeoutSeconds = sendDrainTimeoutSeconds;
        this.preStopWaitSeconds = preStopWaitSeconds;
    }

    private static Map<ProxyLifecycleState, EnumSet<ProxyLifecycleState>> legalTransitions() {
        Map<ProxyLifecycleState, EnumSet<ProxyLifecycleState>> m = new EnumMap<>(ProxyLifecycleState.class);
        m.put(ProxyLifecycleState.STARTING, EnumSet.of(ProxyLifecycleState.READY,
            ProxyLifecycleState.QUIESCING, ProxyLifecycleState.FORCE_DRAINING));
        m.put(ProxyLifecycleState.READY, EnumSet.of(ProxyLifecycleState.QUIESCING,
            ProxyLifecycleState.FORCE_DRAINING));
        m.put(ProxyLifecycleState.QUIESCING, EnumSet.of(ProxyLifecycleState.MIGRATING,
            ProxyLifecycleState.FORCE_DRAINING));
        m.put(ProxyLifecycleState.MIGRATING, EnumSet.of(ProxyLifecycleState.DRAINING,
            ProxyLifecycleState.FORCE_DRAINING));
        m.put(ProxyLifecycleState.DRAINING, EnumSet.of(ProxyLifecycleState.DRAINED,
            ProxyLifecycleState.FORCE_DRAINING));
        m.put(ProxyLifecycleState.DRAINED, EnumSet.of(ProxyLifecycleState.STOPPING));
        m.put(ProxyLifecycleState.FORCE_DRAINING, EnumSet.of(ProxyLifecycleState.STOPPING));
        m.put(ProxyLifecycleState.STOPPING, EnumSet.of(ProxyLifecycleState.STOPPED));
        return m;
    }

    public ProxyLifecycleState state() {
        return state.get();
    }

    void markStarted() {
        reasonRef.set("started");
    }

    void markReady() {
        transition(ProxyLifecycleState.STARTING, ProxyLifecycleState.READY, "ready");
    }

    /**
     * Validated transition. Only the CAS winner from {@code expected} advances the
     * state; a real backward edge, a skipped normal edge, or a duplicate push
     * throws so it is never silently swallowed.
     */
    boolean transition(ProxyLifecycleState expected, ProxyLifecycleState next, String reason) {
        EnumSet<ProxyLifecycleState> allowed = LEGAL.get(expected);
        if (allowed == null || !allowed.contains(next)) {
            throw new IllegalStateException("illegal lifecycle transition " + expected + " -> " + next);
        }
        if (state.compareAndSet(expected, next)) {
            reasonRef.set(reason);
            return true;
        }
        return false;
    }

    @Override
    public void markFatal(String component, Throwable cause) {
        fatalRef.compareAndSet(null, cause);
        reasonRef.set("fatal:" + component);
    }

    @Override
    public boolean isStarted() {
        return state.get() != ProxyLifecycleState.STARTING || "started".equals(reasonRef.get());
    }

    @Override
    public boolean isLive() {
        return fatalRef.get() == null && state.get() != ProxyLifecycleState.STOPPED;
    }

    @Override
    public boolean isReady() {
        ProxyLifecycleState s = state.get();
        return fatalRef.get() == null && s == ProxyLifecycleState.READY;
    }

    @Override
    public ProxyLifecycleSnapshot snapshot() {
        DrainRun run = drainRunRef.get();
        ProxyLifecycleState s = state.get();
        boolean forced = s == ProxyLifecycleState.FORCE_DRAINING;
        return new ProxyLifecycleSnapshot(s, reasonRef.get(), lifecycleEnabled,
            run == null ? null : run.session().drainId(), forced,
            gate.acceptedCount(), gate.isAdmissionClosed());
    }

    @Override
    public DrainRun beginDrain(DrainTrigger trigger) {
        DrainRun existing = drainRunRef.get();
        if (existing != null) {
            return existing;
        }
        long now = nanoClock.getAsLong();
        DrainSession session = DrainSession.fromSeconds(newDrainId(trigger, now), trigger, now,
            Instant.EPOCH, lbDetachTimeoutSeconds, connectionLeaseSeconds, remotingLeaseJitterRatio,
            connectionLeaseGraceSeconds, sendDrainTimeoutSeconds, preStopWaitSeconds);
        DrainRun candidate = new DrainRun(session, new CompletableFuture<>());
        if (!drainRunRef.compareAndSet(null, candidate)) {
            return drainRunRef.get();
        }
        scheduler.execute(() -> orchestrate(candidate));
        return candidate;
    }

    private static String newDrainId(DrainTrigger trigger, long now) {
        return trigger.name().toLowerCase() + "-" + Long.toUnsignedString(now);
    }

    /** Active lb cutoff in nanos, or 0 when no drain is running (nothing is late yet). */
    public long activeLbCutoffNanos() {
        DrainRun run = drainRunRef.get();
        return run == null ? 0L : run.session().lbCutoffNanos();
    }

    boolean isForced() {
        return state.get() == ProxyLifecycleState.FORCE_DRAINING;
    }

    private ShutdownDeadline effectiveDrainDeadline(DrainSession session) {
        ShutdownDeadline override = stopOverride.get();
        long hard = session.hardDeadlineNanos();
        if (override != null && override.deadlineNanos() - hard < 0L) {
            return override;
        }
        return new ShutdownDeadline(hard, nanoClock);
    }

    /**
     * Drives the single closing order for the sole active drain. Migration is
     * started on every adapter with the gate still open; once every adapter
     * reaches no-new-work the shared gate is closed exactly once; then accepted
     * sends drain and each adapter's transport terminates. Any failure or a
     * deadline breach flips to FORCE_DRAINING.
     */
    private void orchestrate(DrainRun run) {
        DrainSession session = run.session();
        CompletableFuture<DrainResult> future = run.drainFuture();
        try {
            if (state.get() == ProxyLifecycleState.STARTING) {
                // Never served business: escalate straight to forced teardown.
                forceDrain(run, "starting_state", null);
                return;
            }
            transition(ProxyLifecycleState.READY, ProxyLifecycleState.QUIESCING, "quiescing");
            if (readinessWithdraw != null) {
                readinessWithdraw.run();
            }
            // Hold in QUIESCING until the lb cutoff so the provider (EndpointSlice plus
            // NLB target deregistration) can stop routing new connections here. Migrating
            // earlier would let a client that reconnects after GOAWAY land back on this Pod.
            lbTimer = scheduler.schedule(() -> continueToMigrating(run),
                Math.max(0L, session.lbCutoffNanos() - nanoClock.getAsLong()));
            scheduleHardDeadline(run, session);
        } catch (Throwable t) {
            forceDrain(run, "orchestrate_exception", t);
        }
    }

    /**
     * Second half of the drain, run once the lb cutoff has elapsed: issue migration to
     * every adapter, then close admission after they all report no-new-work.
     */
    private void continueToMigrating(DrainRun run) {
        DrainSession session = run.session();
        CompletableFuture<DrainResult> future = run.drainFuture();
        try {
            if (!transition(ProxyLifecycleState.QUIESCING, ProxyLifecycleState.MIGRATING, "migrating")) {
                // Already advanced (forced, or pre-empted by a direct TERM): nothing to do.
                return;
            }
            for (DrainProtocolAdapter adapter : adapters) {
                adapter.startMigration();
            }

            CompletableFuture<Void> allNoNewWork = allOf(adapters, DrainProtocolAdapter::noNewWorkReached);
            allNoNewWork
                .thenCompose(ignored -> {
                    transition(ProxyLifecycleState.MIGRATING, ProxyLifecycleState.DRAINING, "draining");
                    gate.closeAdmission();
                    return gate.drainedFuture();
                })
                .thenCompose(ignored -> {
                    ShutdownDeadline deadline = effectiveDrainDeadline(session);
                    return allOf(adapters, adapter -> adapter.awaitTerminated(deadline));
                })
                .whenComplete((ignored, error) -> {
                    if (error != null) {
                        forceDrain(run, "drain_error", error);
                        return;
                    }
                    if (transition(ProxyLifecycleState.DRAINING, ProxyLifecycleState.DRAINED, "drained")) {
                        future.complete(DrainResult.drained());
                    }
                });
        } catch (Throwable t) {
            forceDrain(run, "migrate_exception", t);
        }
    }

    private void scheduleHardDeadline(DrainRun run, DrainSession session) {
        long delay = session.hardDeadlineNanos() - nanoClock.getAsLong();
        hardDeadlineTimer = scheduler.schedule(() -> {
            if (!run.drainFuture().isDone()) {
                forceDrain(run, "hard_deadline_exceeded", null);
            }
        }, Math.max(0L, delay));
    }

    private void forceDrain(DrainRun run, String reason, Throwable cause) {
        ProxyLifecycleState cur = state.get();
        if (cur == ProxyLifecycleState.FORCE_DRAINING || cur == ProxyLifecycleState.DRAINED) {
            return;
        }
        if (!state.compareAndSet(cur, ProxyLifecycleState.FORCE_DRAINING)) {
            return;
        }
        reasonRef.set("forced:" + reason);
        ShutdownDeadline deadline = effectiveDrainDeadline(run.session());
        for (DrainProtocolAdapter adapter : adapters) {
            try {
                adapter.force(deadline);
            } catch (Throwable ignored) {
                // best-effort: keep forcing the remaining adapters
            }
        }
        List<Throwable> causes = cause == null ? java.util.Collections.emptyList()
            : java.util.Collections.singletonList(cause);
        run.drainFuture().complete(DrainResult.forced(reason, causes));
    }

    private static CompletableFuture<Void> allOf(List<DrainProtocolAdapter> adapters,
        java.util.function.Function<DrainProtocolAdapter, CompletableFuture<Void>> fn) {
        CompletableFuture<?>[] futures = new CompletableFuture<?>[adapters.size()];
        for (int i = 0; i < adapters.size(); i++) {
            futures[i] = fn.apply(adapters.get(i));
        }
        return CompletableFuture.allOf(futures);
    }

    /**
     * Direct-TERM escalation: cancels the pending lb/migration timers, records the
     * tighter stop deadline, and fast-forwards the current state to DRAINING so the
     * shared gate closes and every adapter is frozen without waiting out the lb or
     * migration cutoffs. The returned future completes once migration has been
     * issued to every adapter.
     */
    CompletableFuture<Void> escalateForStop(ShutdownDeadline stopDeadline) {
        stopOverride.set(stopDeadline);
        phaseGeneration.incrementAndGet();
        cancel(lbTimer);
        cancel(hardDeadlineTimer);
        DrainRun run = drainRunRef.get();
        CompletableFuture<Void> issued = new CompletableFuture<>();
        scheduler.execute(() -> {
            try {
                ProxyLifecycleState cur = state.get();
                if (cur == ProxyLifecycleState.STARTING) {
                    if (run != null) {
                        forceDrain(run, "term_before_ready", null);
                    }
                    issued.complete(null);
                    return;
                }
                gate.closeAdmission();
                for (DrainProtocolAdapter adapter : adapters) {
                    adapter.startMigration();
                    adapter.force(stopDeadline);
                }
                issued.complete(null);
            } catch (Throwable t) {
                issued.completeExceptionally(t);
            }
        });
        return issued;
    }

    private static void cancel(LifecycleScheduler.ScheduledHandle handle) {
        if (handle != null) {
            handle.cancel();
        }
    }
}
