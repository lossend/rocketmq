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
package org.apache.rocketmq.proxy.service.admin;

import java.util.Collections;
import java.util.HashMap;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for the createSubscriptionGroup / deleteSubscriptionGroup methods added to support
 * traffic-label consumer group management.
 *
 * <p>Uses the package-private {@link BrokerSubscriptionOps} injection point on
 * {@link DefaultAdminService} so that the broker-level calls can be verified without
 * constructing a real {@link org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt}
 * (which would spin up a Netty client and is not mockable with the project's Mockito version).
 */
public class DefaultAdminServiceTrafficLabelTest {

    private BrokerSubscriptionOps brokerSubscriptionOps;
    private DefaultAdminService adminService;

    @Before
    public void setUp() throws Exception {
        brokerSubscriptionOps = mock(BrokerSubscriptionOps.class);

        TopicRouteData route = new TopicRouteData();
        BrokerData broker = new BrokerData();
        broker.setBrokerName("broker-a");
        HashMap<Long, String> addrs = new HashMap<>();
        addrs.put(0L, "127.0.0.1:10911");
        broker.setBrokerAddrs(addrs);
        route.setBrokerDatas(Collections.singletonList(broker));

        adminService = new DefaultAdminService(null, brokerSubscriptionOps) {
            @Override
            protected TopicRouteData getTopicRouteDataDirectlyFromNameServer(String topic) {
                return route;
            }
        };
    }

    @Test
    public void creates_subscription_group_on_each_broker() throws Exception {
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName("G%gray1");

        boolean ok = adminService.createSubscriptionGroup("sample-topic", config);

        assertThat(ok).isTrue();
        ArgumentCaptor<SubscriptionGroupConfig> captor =
            ArgumentCaptor.forClass(SubscriptionGroupConfig.class);
        verify(brokerSubscriptionOps, times(1))
            .createSubscriptionGroup(eq("127.0.0.1:10911"), captor.capture(), anyLong());
        assertThat(captor.getValue().getGroupName()).isEqualTo("G%gray1");
    }

    @Test
    public void deletes_subscription_group_on_each_broker() throws Exception {
        boolean ok = adminService.deleteSubscriptionGroup("sample-topic", "G%gray1");

        assertThat(ok).isTrue();
        verify(brokerSubscriptionOps, times(1))
            .deleteSubscriptionGroup(eq("127.0.0.1:10911"), eq("G%gray1"), eq(true), anyLong());
    }
}
