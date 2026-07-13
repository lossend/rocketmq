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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    @Test
    public void clones_gray_group_onto_all_cluster_masters_regardless_of_source_presence() throws Exception {
        SubscriptionGroupConfig masterAConfig = subscriptionGroup("G", 3, "master-a");
        SubscriptionGroupConfig masterBConfig = subscriptionGroup("G", 7, "master-b");
        ClusterInfo clusterInfo = clusterInfo();

        adminService = new DefaultAdminService(null, brokerSubscriptionOps) {
            @Override
            protected ClusterInfo getBrokerClusterInfo() {
                return clusterInfo;
            }

            @Override
            protected String getRocketMQClusterName() {
                return "cluster-test";
            }
        };
        // master-a and master-b host the source group; master-c hosts neither source nor target.
        when(brokerSubscriptionOps.getAllSubscriptionGroup(eq("master-a:10911"), anyLong()))
            .thenReturn(subscriptionGroups(masterAConfig));
        when(brokerSubscriptionOps.getAllSubscriptionGroup(eq("master-b:10911"), anyLong()))
            .thenReturn(subscriptionGroups(masterBConfig));
        when(brokerSubscriptionOps.getAllSubscriptionGroup(eq("master-c:10911"), anyLong()))
            .thenReturn(subscriptionGroups());

        assertThat(adminService.cloneSubscriptionGroupIfAbsent("G", "G%gray1")).isTrue();

        // The gray group must be created on every master, including master-c which lacks the source.
        ArgumentCaptor<String> addressCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<SubscriptionGroupConfig> configCaptor = ArgumentCaptor.forClass(SubscriptionGroupConfig.class);
        verify(brokerSubscriptionOps, times(3)).createSubscriptionGroup(
            addressCaptor.capture(), configCaptor.capture(), anyLong());
        verify(brokerSubscriptionOps, times(1)).createSubscriptionGroup(
            eq("master-c:10911"), org.mockito.ArgumentMatchers.any(SubscriptionGroupConfig.class), anyLong());
        verify(brokerSubscriptionOps, never()).getAllSubscriptionGroup(eq("slave-a:10912"), anyLong());

        // The config sourced from the first master that has it (master-a) is applied everywhere.
        SubscriptionGroupConfig sourceConfig = masterAConfig;
        Map<String, SubscriptionGroupConfig> copiesByAddress = new HashMap<>();
        for (int i = 0; i < addressCaptor.getAllValues().size(); i++) {
            copiesByAddress.put(addressCaptor.getAllValues().get(i), configCaptor.getAllValues().get(i));
        }
        assertCopied(sourceConfig, copiesByAddress.get("master-a:10911"));
        assertCopied(sourceConfig, copiesByAddress.get("master-b:10911"));
        assertCopied(sourceConfig, copiesByAddress.get("master-c:10911"));
    }

    @Test
    public void does_not_overwrite_existing_gray_group_on_master_that_has_it() throws Exception {
        SubscriptionGroupConfig source = subscriptionGroup("G", 3, "source");
        SubscriptionGroupConfig existingGray = subscriptionGroup("G%gray1", 99, "existing");
        ClusterInfo clusterInfo = clusterInfo();

        adminService = new DefaultAdminService(null, brokerSubscriptionOps) {
            @Override
            protected ClusterInfo getBrokerClusterInfo() {
                return clusterInfo;
            }

            @Override
            protected String getRocketMQClusterName() {
                return "cluster-test";
            }
        };
        // master-a already has both source and target; master-b and master-c have neither.
        when(brokerSubscriptionOps.getAllSubscriptionGroup(eq("master-a:10911"), anyLong()))
            .thenReturn(subscriptionGroups(source, existingGray));
        when(brokerSubscriptionOps.getAllSubscriptionGroup(eq("master-b:10911"), anyLong()))
            .thenReturn(subscriptionGroups());
        when(brokerSubscriptionOps.getAllSubscriptionGroup(eq("master-c:10911"), anyLong()))
            .thenReturn(subscriptionGroups());

        assertThat(adminService.cloneSubscriptionGroupIfAbsent("G", "G%gray1")).isTrue();

        // master-a keeps its existing gray group; master-b and master-c receive a fresh create.
        verify(brokerSubscriptionOps, never()).createSubscriptionGroup(
            eq("master-a:10911"), org.mockito.ArgumentMatchers.any(SubscriptionGroupConfig.class), anyLong());
        verify(brokerSubscriptionOps, times(1)).createSubscriptionGroup(
            eq("master-b:10911"), org.mockito.ArgumentMatchers.any(SubscriptionGroupConfig.class), anyLong());
        verify(brokerSubscriptionOps, times(1)).createSubscriptionGroup(
            eq("master-c:10911"), org.mockito.ArgumentMatchers.any(SubscriptionGroupConfig.class), anyLong());
    }

    @Test
    public void returns_false_when_source_group_exists_on_no_master() throws Exception {
        ClusterInfo clusterInfo = clusterInfo();

        adminService = new DefaultAdminService(null, brokerSubscriptionOps) {
            @Override
            protected ClusterInfo getBrokerClusterInfo() {
                return clusterInfo;
            }

            @Override
            protected String getRocketMQClusterName() {
                return "cluster-test";
            }
        };
        when(brokerSubscriptionOps.getAllSubscriptionGroup(eq("master-a:10911"), anyLong()))
            .thenReturn(subscriptionGroups());
        when(brokerSubscriptionOps.getAllSubscriptionGroup(eq("master-b:10911"), anyLong()))
            .thenReturn(subscriptionGroups());
        when(brokerSubscriptionOps.getAllSubscriptionGroup(eq("master-c:10911"), anyLong()))
            .thenReturn(subscriptionGroups());

        assertThat(adminService.cloneSubscriptionGroupIfAbsent("G", "G%gray1")).isFalse();

        verify(brokerSubscriptionOps, never()).createSubscriptionGroup(
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.any(SubscriptionGroupConfig.class), anyLong());
    }

    @Test
    public void returns_false_when_a_master_create_throws() throws Exception {
        SubscriptionGroupConfig source = subscriptionGroup("G", 3, "source");
        ClusterInfo clusterInfo = clusterInfo();

        adminService = new DefaultAdminService(null, brokerSubscriptionOps) {
            @Override
            protected ClusterInfo getBrokerClusterInfo() {
                return clusterInfo;
            }

            @Override
            protected String getRocketMQClusterName() {
                return "cluster-test";
            }
        };
        // Only master-a hosts the source group; the create on master-b fails.
        when(brokerSubscriptionOps.getAllSubscriptionGroup(eq("master-a:10911"), anyLong()))
            .thenReturn(subscriptionGroups(source));
        when(brokerSubscriptionOps.getAllSubscriptionGroup(eq("master-b:10911"), anyLong()))
            .thenReturn(subscriptionGroups());
        when(brokerSubscriptionOps.getAllSubscriptionGroup(eq("master-c:10911"), anyLong()))
            .thenReturn(subscriptionGroups());
        org.mockito.Mockito.doThrow(new RuntimeException("broker unreachable"))
            .when(brokerSubscriptionOps).createSubscriptionGroup(
                eq("master-b:10911"), org.mockito.ArgumentMatchers.any(SubscriptionGroupConfig.class), anyLong());

        assertThat(adminService.cloneSubscriptionGroupIfAbsent("G", "G%gray1")).isFalse();
    }

    private static SubscriptionGroupConfig subscriptionGroup(String name, int retryQueueNums, String marker) {
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(name);
        config.setRetryQueueNums(retryQueueNums);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("marker", marker);
        config.setAttributes(attributes);
        return config;
    }

    private static SubscriptionGroupWrapper subscriptionGroups(SubscriptionGroupConfig... configs) {
        SubscriptionGroupWrapper wrapper = new SubscriptionGroupWrapper();
        ConcurrentHashMap<String, SubscriptionGroupConfig> groups = new ConcurrentHashMap<>();
        for (SubscriptionGroupConfig config : configs) {
            groups.put(config.getGroupName(), config);
        }
        wrapper.setSubscriptionGroupTable(groups);
        return wrapper;
    }

    private static ClusterInfo clusterInfo() {
        ClusterInfo clusterInfo = new ClusterInfo();
        Map<String, BrokerData> brokers = new HashMap<>();
        brokers.put("broker-a", brokerData("broker-a", "master-a:10911", "slave-a:10912"));
        brokers.put("broker-b", brokerData("broker-b", "master-b:10911", null));
        brokers.put("broker-c", brokerData("broker-c", "master-c:10911", null));
        clusterInfo.setBrokerAddrTable(brokers);
        Map<String, java.util.Set<String>> clusters = new HashMap<>();
        clusters.put("cluster-test", new LinkedHashSet<>(Arrays.asList("broker-a", "broker-b", "broker-c")));
        clusterInfo.setClusterAddrTable(clusters);
        return clusterInfo;
    }

    private static BrokerData brokerData(String name, String master, String slave) {
        BrokerData broker = new BrokerData();
        broker.setBrokerName(name);
        HashMap<Long, String> addrs = new HashMap<>();
        addrs.put(0L, master);
        if (slave != null) {
            addrs.put(1L, slave);
        }
        broker.setBrokerAddrs(addrs);
        return broker;
    }

    private static void assertCopied(SubscriptionGroupConfig source, SubscriptionGroupConfig copy) {
        assertThat(copy).isNotSameAs(source);
        assertThat(copy.getGroupName()).isEqualTo("G%gray1");
        assertThat(copy.getRetryQueueNums()).isEqualTo(source.getRetryQueueNums());
        assertThat(copy.getAttributes()).containsExactlyEntriesOf(source.getAttributes());
        assertThat(copy.getAttributes()).isNotSameAs(source.getAttributes());
    }
}
