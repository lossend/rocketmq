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
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LabelGroupCleanerTest {

    private final long threshold = 1000L;

    private LabelGroupCleaner.CleanupProbe probe(boolean online, boolean drained, long offlineMs) {
        LabelGroupCleaner.CleanupProbe p = mock(LabelGroupCleaner.CleanupProbe.class);
        when(p.hasOnlineConsumer("G%gray1")).thenReturn(online);
        when(p.isDrained("test-topic", "G%gray1")).thenReturn(drained);
        when(p.offlineDurationMs("G%gray1")).thenReturn(offlineMs);
        return p;
    }

    @Test
    public void deletes_idle_drained_offline_group() {
        AdminService admin = mock(AdminService.class);
        LabelGroupCleaner cleaner = new LabelGroupCleaner(admin, threshold);
        cleaner.cleanupOnce("test-topic", java.util.Collections.singletonList("G%gray1"),
            probe(false, true, 2000L));
        verify(admin).deleteSubscriptionGroup("test-topic", "G%gray1");
    }

    @Test
    public void keeps_group_with_online_consumer() {
        AdminService admin = mock(AdminService.class);
        LabelGroupCleaner cleaner = new LabelGroupCleaner(admin, threshold);
        cleaner.cleanupOnce("test-topic", java.util.Collections.singletonList("G%gray1"),
            probe(true, true, 2000L));
        verify(admin, never()).deleteSubscriptionGroup(any(), any());
    }

    @Test
    public void keeps_group_offline_within_threshold() {
        AdminService admin = mock(AdminService.class);
        LabelGroupCleaner cleaner = new LabelGroupCleaner(admin, threshold);
        cleaner.cleanupOnce("test-topic", java.util.Collections.singletonList("G%gray1"),
            probe(false, true, 500L));
        verify(admin, never()).deleteSubscriptionGroup(any(), any());
    }

    @Test
    public void keeps_group_not_yet_drained() {
        AdminService admin = mock(AdminService.class);
        LabelGroupCleaner cleaner = new LabelGroupCleaner(admin, threshold);
        cleaner.cleanupOnce("test-topic", java.util.Collections.singletonList("G%gray1"),
            probe(false, false, 2000L));
        verify(admin, never()).deleteSubscriptionGroup(any(), any());
    }
}
