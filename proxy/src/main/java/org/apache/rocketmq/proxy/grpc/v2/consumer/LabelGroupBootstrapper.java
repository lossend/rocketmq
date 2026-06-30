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

package org.apache.rocketmq.proxy.grpc.v2.consumer;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

/**
 * Lazily creates {@code G%<label>} subscription groups on first use and caches successful creations
 * to avoid redundant broker calls on subsequent requests.
 *
 * <p>A creation failure (when {@link AdminService#createSubscriptionGroup} returns {@code false})
 * is not cached, so the next call for the same group will retry the creation.
 */
public class LabelGroupBootstrapper {

    private final AdminService adminService;
    private final Set<String> createdGroups = ConcurrentHashMap.newKeySet();

    /**
     * Constructs a bootstrapper backed by the given {@link AdminService}.
     *
     * @param adminService the admin service used to create subscription groups on brokers
     */
    public LabelGroupBootstrapper(AdminService adminService) {
        this.adminService = adminService;
    }

    /**
     * Ensures that the specified subscription group exists on the brokers serving the given topic.
     * If the group was already successfully created in a previous call it is returned from the
     * in-memory cache and no broker call is made.  If the previous attempt failed (or this is the
     * first call) the creation is attempted via {@link AdminService#createSubscriptionGroup};
     * only a successful result is cached.
     *
     * @param sampleTopic    topic used to discover the target brokers via NameServer route lookup
     * @param effectiveGroup subscription group name (e.g. {@code G%gray1}) to create/ensure
     */
    public void ensureGroup(String sampleTopic, String effectiveGroup) {
        if (createdGroups.contains(effectiveGroup)) {
            return;
        }
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(effectiveGroup);
        boolean ok = adminService.createSubscriptionGroup(sampleTopic, config);
        if (ok) {
            createdGroups.add(effectiveGroup);
        }
    }
}
