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
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LabelGroupBootstrapperTest {

    @Test
    public void delegates_gray_group_creation_to_distribution_aware_admin_service() {
        AdminService admin = mock(AdminService.class);
        when(admin.cloneSubscriptionGroupIfAbsent("G", "G%gray1")).thenReturn(true);
        LabelGroupBootstrapper boot = new LabelGroupBootstrapper(admin);

        boolean created = boot.ensureGrayGroupFromOrigin("G", "G%gray1");

        assertThat(created).isTrue();
        verify(admin, times(1)).cloneSubscriptionGroupIfAbsent(eq("G"), eq("G%gray1"));
    }

    @Test
    public void does_not_cache_failed_distribution_copy() {
        AdminService admin = mock(AdminService.class);
        when(admin.cloneSubscriptionGroupIfAbsent("G", "G%gray1")).thenReturn(false, true);
        LabelGroupBootstrapper boot = new LabelGroupBootstrapper(admin);

        assertThat(boot.ensureGrayGroupFromOrigin("G", "G%gray1")).isFalse();
        assertThat(boot.ensureGrayGroupFromOrigin("G", "G%gray1")).isTrue();

        verify(admin, times(2)).cloneSubscriptionGroupIfAbsent(eq("G"), eq("G%gray1"));
    }

    @Test
    public void invalidates_cache_after_successful_ensure() {
        AdminService admin = mock(AdminService.class);
        MetadataService metadata = mock(MetadataService.class);
        when(admin.cloneSubscriptionGroupIfAbsent("G", "G%gray1")).thenReturn(true);
        LabelGroupBootstrapper boot = new LabelGroupBootstrapper(admin, metadata);

        boot.ensureGrayGroupFromOrigin("G", "G%gray1");

        verify(metadata, times(1)).invalidateSubscriptionGroupConfig(eq("G%gray1"));
    }

    @Test
    public void does_not_invalidate_cache_when_ensure_fails() {
        AdminService admin = mock(AdminService.class);
        MetadataService metadata = mock(MetadataService.class);
        when(admin.cloneSubscriptionGroupIfAbsent("G", "G%gray1")).thenReturn(false);
        LabelGroupBootstrapper boot = new LabelGroupBootstrapper(admin, metadata);

        boot.ensureGrayGroupFromOrigin("G", "G%gray1");

        verify(metadata, never()).invalidateSubscriptionGroupConfig(any());
    }
}
