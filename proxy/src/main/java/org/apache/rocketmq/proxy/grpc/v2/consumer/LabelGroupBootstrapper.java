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

import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;

/**
 * Creates {@code G%<label>} subscription groups from their source group on the exact masters that
 * contain that source group.
 *
 * <p>The broker topology and source-group distribution are rechecked for every registration. This
 * lets a later registration repair a deleted gray group or cover a newly added source broker.
 */
public class LabelGroupBootstrapper {

    private final AdminService adminService;
    private final MetadataService metadataService;

    /**
     * Constructs a bootstrapper backed by the given {@link AdminService} without cache invalidation.
     *
     * @param adminService the admin service used to create subscription groups on brokers
     */
    public LabelGroupBootstrapper(AdminService adminService) {
        this(adminService, null);
    }

    /**
     * Constructs a bootstrapper backed by the given {@link AdminService} that also invalidates the
     * subscription group config cache after a group is successfully created.
     *
     * @param adminService    the admin service used to create subscription groups on brokers
     * @param metadataService the metadata service whose subscription group cache is invalidated
     *                        after a successful create; may be {@code null} to disable invalidation
     */
    public LabelGroupBootstrapper(AdminService adminService, MetadataService metadataService) {
        this.adminService = adminService;
        this.metadataService = metadataService;
    }

    /**
     * Ensures that the gray group mirrors the original group's Master-broker distribution.
     *
     * <p>When a group is newly created and a {@link MetadataService} was supplied, the stale
     * (possibly negative) cache entry for the effective group is invalidated so that the next
     * lookup reflects the freshly created group.
     *
     * @param originGroup    original subscription group name
     * @param effectiveGroup gray subscription group name (e.g. {@code G%gray1}) to create/ensure
     * @return {@code true} when every required Master has a gray group and the source exists
     */
    public boolean ensureGrayGroupFromOrigin(String originGroup, String effectiveGroup) {
        boolean created = adminService.cloneSubscriptionGroupIfAbsent(originGroup, effectiveGroup);
        if (created && metadataService != null) {
            metadataService.invalidateSubscriptionGroupConfig(effectiveGroup);
        }
        return created;
    }
}
