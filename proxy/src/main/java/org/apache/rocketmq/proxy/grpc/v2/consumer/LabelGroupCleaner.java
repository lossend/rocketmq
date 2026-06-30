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

import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.service.admin.AdminService;

public class LabelGroupCleaner {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final AdminService adminService;
    private final long idleThresholdMs;

    public LabelGroupCleaner(AdminService adminService, long idleThresholdMs) {
        this.adminService = adminService;
        this.idleThresholdMs = idleThresholdMs;
    }

    public void cleanupOnce(String sampleTopic, List<String> candidateGroups, CleanupProbe probe) {
        for (String group : candidateGroups) {
            if (probe.hasOnlineConsumer(group)) {
                continue;
            }
            if (!probe.isDrained(sampleTopic, group)) {
                continue;
            }
            if (probe.offlineDurationMs(group) < idleThresholdMs) {
                continue;
            }
            boolean ok = adminService.deleteSubscriptionGroup(sampleTopic, group);
            log.info("traffic-label cleanup delete group {} result={}", group, ok);
        }
    }

    public interface CleanupProbe {
        boolean hasOnlineConsumer(String group);
        boolean isDrained(String sampleTopic, String group);
        long offlineDurationMs(String group);
    }
}
