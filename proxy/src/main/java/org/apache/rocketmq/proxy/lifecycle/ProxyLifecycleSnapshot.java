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

/**
 * Immutable point-in-time view of the coordinator, for the admin {@code /state}
 * endpoint and metrics gauges.
 */
public final class ProxyLifecycleSnapshot {

    private final ProxyLifecycleState state;
    private final String reason;
    private final boolean lifecycleEnabled;
    private final String drainId;
    private final boolean forced;
    private final long acceptedSends;
    private final boolean admissionClosed;

    public ProxyLifecycleSnapshot(ProxyLifecycleState state, String reason, boolean lifecycleEnabled,
        String drainId, boolean forced, long acceptedSends, boolean admissionClosed) {
        this.state = state;
        this.reason = reason;
        this.lifecycleEnabled = lifecycleEnabled;
        this.drainId = drainId;
        this.forced = forced;
        this.acceptedSends = acceptedSends;
        this.admissionClosed = admissionClosed;
    }

    public ProxyLifecycleState state() {
        return state;
    }

    public String reason() {
        return reason;
    }

    public boolean lifecycleEnabled() {
        return lifecycleEnabled;
    }

    public String drainId() {
        return drainId;
    }

    public boolean forced() {
        return forced;
    }

    public long acceptedSends() {
        return acceptedSends;
    }

    public boolean admissionClosed() {
        return admissionClosed;
    }
}
