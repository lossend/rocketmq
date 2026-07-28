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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

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

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

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

    // Provider-health barrier backing isReadyForTraffic(). Null until warmup wiring
    // installs it; when null the traffic predicate falls back to isReady() so an
    // unwired coordinator never regresses.
    private volatile ReadinessBarrier readinessBarrier;

    // Optional lb-detach quiet-window observation. Diagnostic only: it never shortens
    // the wait, because a locally idle listener does not prove the provider has
    // deregistered this target. Its purpose is to supply real data for calibrating
    // proxyLbDetachTimeoutSeconds.
    private volatile LongSupplier quietDurationNanosSupplier;
    private volatile int lbDetachQuietSeconds;
    private volatile ProxyLifecycleMetrics metrics;

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

    /**
     * Installs the lb-detach quiet-window observation. Purely diagnostic: the drain
     * always waits the full lb cutoff regardless of what is observed, since an idle
     * local listener is not evidence that the provider stopped routing new
     * connections here.
     *
     * @param quietDurationNanosSupplier nanos since the last business connect, or -1 if none seen
     * @param lbDetachQuietSeconds       quiet threshold to report against
     * @param metrics                    optional metrics sink
     */
    public void observeLbDetachQuiet(LongSupplier quietDurationNanosSupplier,
        int lbDetachQuietSeconds, ProxyLifecycleMetrics metrics) {
        this.quietDurationNanosSupplier = quietDurationNanosSupplier;
        this.lbDetachQuietSeconds = lbDetachQuietSeconds;
        this.metrics = metrics;
    }

    /**
     * Reports whether the listener had gone quiet by the lb cutoff. A quiet window at
     * least as long as the threshold suggests the provider finished deregistering well
     * before the cutoff (the budget has headroom); a short window suggests new
     * connections were still arriving and {@code proxyLbDetachTimeoutSeconds} may be
     * too small.
     */
    private void reportLbDetachQuiet(DrainRun run) {
        LongSupplier supplier = quietDurationNanosSupplier;
        if (supplier == null) {
            return;
        }
        long quietNanos = supplier.getAsLong();
        String drainId = run.session().drainId();
        if (quietNanos < 0L) {
            log.info("lb detach quiet report drainId={} quiet=none-observed", drainId);
            return;
        }
        long quietSeconds = TimeUnit.NANOSECONDS.toSeconds(quietNanos);
        boolean reachedThreshold = quietSeconds >= lbDetachQuietSeconds;
        log.info("lb detach quiet report drainId={} quietSeconds={} thresholdSeconds={} reached={}",
            drainId, quietSeconds, lbDetachQuietSeconds, reachedThreshold);
        if (!reachedThreshold) {
            log.warn("new business connections arrived within {}s of the lb cutoff (quietSeconds={}); "
                + "proxyLbDetachTimeoutSeconds may be too small", lbDetachQuietSeconds, quietSeconds);
            ProxyLifecycleMetrics sink = metrics;
            if (sink != null) {
                sink.recordLbDetachQuietMissed();
            }
        }
    }

    /**
     * Publishes readiness after every startup component has completed successfully.
     * This is deliberately a single transition so callers cannot expose a partially
     * started process as ready.
     */
    public void onStartupComplete() {
        Throwable fatal = fatalRef.get();
        if (fatal != null) {
            throw new IllegalStateException("cannot become ready after a fatal startup failure", fatal);
        }
        if (!transition(ProxyLifecycleState.STARTING, ProxyLifecycleState.READY, "ready")) {
            throw new IllegalStateException("cannot complete startup from lifecycle state " + state.get());
        }
    }

    /**
     * Begins the process-stop phase, advancing a settled drain (DRAINED or
     * FORCE_DRAINING) to STOPPING so {@code /state} reports teardown-in-progress.
     * Best-effort and non-throwing: from any other current state this is a no-op, so
     * the SIGTERM shutdown hook can call it unconditionally. The forced outcome
     * remains sticky in {@link #snapshot()} across this edge.
     */
    public void markStopping() {
        ProxyLifecycleState cur = state.get();
        if (cur == ProxyLifecycleState.DRAINED || cur == ProxyLifecycleState.FORCE_DRAINING) {
            transition(cur, ProxyLifecycleState.STOPPING, "stopping");
        }
    }

    /**
     * Publishes the terminal STOPPED state once teardown has finished. Only STOPPING
     * may enter STOPPED; from any other current state this is an idempotent no-op, so
     * the shutdown hook never throws. Should be invoked while the admin server is
     * still serving so the terminal state is observable via {@code /state}.
     */
    public void markStopped() {
        transition(ProxyLifecycleState.STOPPING, ProxyLifecycleState.STOPPED, "stopped");
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
        return state.get() != ProxyLifecycleState.STARTING;
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

    /**
     * Installs the provider-health barrier that backs {@link #isReadyForTraffic()}.
     * Called once by the warmup wiring before READY is published.
     *
     * @param barrier the readiness barrier evaluating shared-dependency health
     */
    public void setReadinessBarrier(ReadinessBarrier barrier) {
        this.readinessBarrier = barrier;
    }

    @Override
    public boolean isReadyForTraffic() {
        if (fatalRef.get() != null || state.get() != ProxyLifecycleState.READY) {
            return false;
        }
        ReadinessBarrier barrier = readinessBarrier;
        return barrier == null || barrier.isReadyForTraffic();
    }

    @Override
    public ProxyLifecycleSnapshot snapshot() {
        DrainRun run = drainRunRef.get();
        ProxyLifecycleState s = state.get();
        // Forced is a sticky outcome of the drain, not merely the current phase.
        // It must remain visible while the process advances through STOPPING/STOPPED.
        boolean forced = s == ProxyLifecycleState.FORCE_DRAINING;
        if (!forced && run != null && run.drainFuture().isDone()
            && !run.drainFuture().isCompletedExceptionally()) {
            forced = run.drainFuture().join().isForced();
        }
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
            // Diagnostic only, emitted at the cutoff we just waited out.
            reportLbDetachQuiet(run);
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
                .thenComposeAsync(ignored -> {
                    ShutdownDeadline deadline = effectiveDrainDeadline(session);
                    return allOf(adapters, adapter -> adapter.awaitTerminated(deadline));
                }, scheduler::execute)
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
        // Close admission before publishing FORCE_DRAINING so no sender can acquire a
        // permit after the forced state becomes visible: closeAdmission() happens-before
        // the state CAS on this thread, so any acquirer observing FORCE_DRAINING sees a
        // closed gate. closeAdmission() is idempotent, so a force call that loses the
        // CAS (state already advanced) does no harm.
        gate.closeAdmission();
        if (!tryEnterForceDraining(state::get, state::compareAndSet)) {
            return;
        }
        reasonRef.set("forced:" + reason);
        ShutdownDeadline deadline = effectiveDrainDeadline(run.session());
        List<Throwable> causes = new ArrayList<>();
        if (cause != null) {
            causes.add(cause);
        }
        for (DrainProtocolAdapter adapter : adapters) {
            try {
                adapter.force(deadline);
            } catch (Throwable forceError) {
                // best-effort: keep forcing the remaining adapters
                causes.add(forceError);
                log.warn("failed to force adapter {} during drain {}", adapter.getClass().getName(),
                    run.session().drainId(), forceError);
            }
        }
        run.drainFuture().complete(DrainResult.forced(reason, causes));
    }

    /**
     * Enters the forced state without losing the hard-deadline signal when a
     * normal lifecycle transition wins between the state read and CAS. Lifecycle
     * states only move forward, so a failed CAS must re-read and retry until this
     * caller wins or observes a state that can no longer be forced.
     *
     * <p>The functional arguments keep the CAS-loss path deterministic in unit
     * tests; production passes the coordinator's atomic state operations.</p>
     */
    static boolean tryEnterForceDraining(Supplier<ProxyLifecycleState> currentState,
        BiPredicate<ProxyLifecycleState, ProxyLifecycleState> compareAndSet) {
        while (true) {
            ProxyLifecycleState current = currentState.get();
            EnumSet<ProxyLifecycleState> allowed = LEGAL.get(current);
            if (allowed == null || !allowed.contains(ProxyLifecycleState.FORCE_DRAINING)) {
                return false;
            }
            if (compareAndSet.test(current, ProxyLifecycleState.FORCE_DRAINING)) {
                return true;
            }
        }
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
