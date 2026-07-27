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

package org.apache.rocketmq.proxy.lifecycle.admin;

import java.util.concurrent.Executor;
import org.apache.rocketmq.proxy.lifecycle.DrainRun;
import org.apache.rocketmq.proxy.lifecycle.DrainTrigger;
import org.apache.rocketmq.proxy.lifecycle.ProxyLifecycle;
import org.apache.rocketmq.proxy.lifecycle.ProxyLifecycleSnapshot;

/**
 * Bridges the {@link ProxyLifecycle} coordinator to the admin HTTP surface. Every
 * handler is short and non-blocking: {@code POST /drain} triggers or reuses the
 * single drain run and returns {@code 202 + runId} without waiting.
 */
public final class CoordinatorAdminHandlers implements ProxyAdminServer.ProxyAdminHandlers {

    private final ProxyLifecycle lifecycle;
    private final Executor executor;

    public CoordinatorAdminHandlers(ProxyLifecycle lifecycle, Executor executor) {
        this.lifecycle = lifecycle;
        this.executor = executor;
    }

    @Override
    public Executor executor() {
        return executor;
    }

    @Override
    public ProxyAdminResponse started() {
        return lifecycle.isStarted()
            ? up("STARTED") : down("STARTING");
    }

    @Override
    public ProxyAdminResponse live() {
        return lifecycle.isLive() ? up("LIVE") : down("DEAD");
    }

    @Override
    public ProxyAdminResponse ready() {
        return lifecycle.isReady() ? up("READY") : down("NOT_READY");
    }

    @Override
    public ProxyAdminResponse readyForTraffic() {
        return lifecycle.isReady() ? up("READY") : down("NOT_READY");
    }

    @Override
    public ProxyAdminResponse state() {
        ProxyLifecycleSnapshot s = lifecycle.snapshot();
        String body = "{"
            + "\"state\":\"" + s.state() + "\","
            + "\"reason\":\"" + escape(s.reason()) + "\","
            + "\"lifecycleEnabled\":" + s.lifecycleEnabled() + ","
            + "\"drainId\":" + quoteOrNull(s.drainId()) + ","
            + "\"forced\":" + s.forced() + ","
            + "\"acceptedSends\":" + s.acceptedSends() + ","
            + "\"admissionClosed\":" + s.admissionClosed()
            + "}";
        return new ProxyAdminResponse(200, body);
    }

    @Override
    public ProxyAdminResponse startDrain() {
        DrainRun run = lifecycle.beginDrain(DrainTrigger.ADMIN);
        return new ProxyAdminResponse(202,
            "{\"runId\":" + quoteOrNull(run.session().drainId()) + "}");
    }

    @Override
    public ProxyAdminResponse drainStatus(String runId) {
        ProxyLifecycleSnapshot s = lifecycle.snapshot();
        if (s.drainId() == null || !s.drainId().equals(runId)) {
            return new ProxyAdminResponse(404, "{\"error\":\"unknown runId\"}");
        }
        String body = "{"
            + "\"runId\":" + quoteOrNull(runId) + ","
            + "\"state\":\"" + s.state() + "\","
            + "\"forced\":" + s.forced() + ","
            + "\"acceptedSends\":" + s.acceptedSends()
            + "}";
        return new ProxyAdminResponse(200, body);
    }

    private static ProxyAdminResponse up(String state) {
        return new ProxyAdminResponse(200, "{\"status\":\"UP\",\"state\":\"" + state + "\"}");
    }

    private static ProxyAdminResponse down(String state) {
        return new ProxyAdminResponse(503, "{\"status\":\"DOWN\",\"state\":\"" + state + "\"}");
    }

    private static String quoteOrNull(String v) {
        return v == null ? "null" : "\"" + escape(v) + "\"";
    }

    private static String escape(String v) {
        return v == null ? "" : v.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
