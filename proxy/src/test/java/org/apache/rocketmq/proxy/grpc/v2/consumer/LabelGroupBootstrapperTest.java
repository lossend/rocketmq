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
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LabelGroupBootstrapperTest {

    @Test
    public void creates_group_once_then_caches() {
        AdminService admin = mock(AdminService.class);
        when(admin.createSubscriptionGroup(any(), any())).thenReturn(true);
        LabelGroupBootstrapper boot = new LabelGroupBootstrapper(admin);

        boot.ensureGroup("test-topic", "G%gray1");
        boot.ensureGroup("test-topic", "G%gray1");

        verify(admin, times(1)).createSubscriptionGroup(eq("test-topic"), any());
    }

    @Test
    public void created_group_name_matches_requested() {
        AdminService admin = mock(AdminService.class);
        when(admin.createSubscriptionGroup(any(), any())).thenReturn(true);
        LabelGroupBootstrapper boot = new LabelGroupBootstrapper(admin);

        boot.ensureGroup("test-topic", "G%gray1");

        ArgumentCaptor<SubscriptionGroupConfig> captor =
            ArgumentCaptor.forClass(SubscriptionGroupConfig.class);
        verify(admin).createSubscriptionGroup(eq("test-topic"), captor.capture());
        assertThat(captor.getValue().getGroupName()).isEqualTo("G%gray1");
    }

    @Test
    public void failed_creation_is_not_cached_and_retries() {
        AdminService admin = mock(AdminService.class);
        when(admin.createSubscriptionGroup(any(), any())).thenReturn(false, true);
        LabelGroupBootstrapper boot = new LabelGroupBootstrapper(admin);

        boot.ensureGroup("test-topic", "G%gray1");
        boot.ensureGroup("test-topic", "G%gray1");

        verify(admin, times(2)).createSubscriptionGroup(eq("test-topic"), any());
    }
}
