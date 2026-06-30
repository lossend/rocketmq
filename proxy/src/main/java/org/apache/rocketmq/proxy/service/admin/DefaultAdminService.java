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

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.proxy.service.route.TopicRouteHelper;

public class DefaultAdminService implements AdminService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final MQClientAPIFactory mqClientAPIFactory;

    /** Thin broker-level ops used for subscription group management; injectable for testing. */
    private final BrokerSubscriptionOps brokerSubscriptionOps;

    public DefaultAdminService(MQClientAPIFactory mqClientAPIFactory) {
        this.mqClientAPIFactory = mqClientAPIFactory;
        this.brokerSubscriptionOps = new DefaultBrokerSubscriptionOps();
    }

    /** Package-private constructor for tests that inject a stub {@link BrokerSubscriptionOps}. */
    DefaultAdminService(MQClientAPIFactory mqClientAPIFactory,
        BrokerSubscriptionOps brokerSubscriptionOps) {
        this.mqClientAPIFactory = mqClientAPIFactory;
        this.brokerSubscriptionOps = brokerSubscriptionOps;
    }

    @Override
    public boolean topicExist(String topic) {
        boolean topicExist;
        TopicRouteData topicRouteData;
        try {
            topicRouteData = this.getTopicRouteDataDirectlyFromNameServer(topic);
            topicExist = topicRouteData != null;
        } catch (Throwable e) {
            topicExist = false;
        }

        return topicExist;
    }

    @Override
    public boolean createTopicOnTopicBrokerIfNotExist(String createTopic, String sampleTopic, int wQueueNum,
        int rQueueNum, boolean examineTopic, int retryCheckCount) {
        TopicRouteData curTopicRouteData = new TopicRouteData();
        try {
            curTopicRouteData = this.getTopicRouteDataDirectlyFromNameServer(createTopic);
        } catch (Exception e) {
            if (!TopicRouteHelper.isTopicNotExistError(e)) {
                log.error("get cur topic route {} failed.", createTopic, e);
                return false;
            }
        }

        TopicRouteData sampleTopicRouteData = null;
        try {
            sampleTopicRouteData = this.getTopicRouteDataDirectlyFromNameServer(sampleTopic);
        } catch (Exception e) {
            log.error("create topic {} failed.", createTopic, e);
            return false;
        }

        if (sampleTopicRouteData == null || sampleTopicRouteData.getBrokerDatas().isEmpty()) {
            return false;
        }

        try {
            return this.createTopicOnBroker(createTopic, wQueueNum, rQueueNum, curTopicRouteData.getBrokerDatas(),
                sampleTopicRouteData.getBrokerDatas(), examineTopic, retryCheckCount);
        } catch (Exception e) {
            log.error("create topic {} failed.", createTopic, e);
        }
        return false;
    }

    @Override
    public boolean createTopicOnBroker(String topic, int wQueueNum, int rQueueNum, List<BrokerData> curBrokerDataList,
        List<BrokerData> sampleBrokerDataList, boolean examineTopic, int retryCheckCount) throws Exception {
        Set<String> curBrokerAddr = new HashSet<>();
        if (curBrokerDataList != null) {
            for (BrokerData brokerData : curBrokerDataList) {
                curBrokerAddr.add(brokerData.getBrokerAddrs().get(MixAll.MASTER_ID));
            }
        }

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topic);
        topicConfig.setWriteQueueNums(wQueueNum);
        topicConfig.setReadQueueNums(rQueueNum);
        topicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);

        for (BrokerData brokerData : sampleBrokerDataList) {
            String addr = brokerData.getBrokerAddrs() == null ? null : brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
            if (addr == null) {
                continue;
            }
            if (curBrokerAddr.contains(addr)) {
                continue;
            }

            try {
                this.getClient().createTopic(addr, TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC, topicConfig, Duration.ofSeconds(3).toMillis());
            } catch (Exception e) {
                log.error("create topic on broker failed. topic:{}, broker:{}", topicConfig, addr, e);
            }
        }

        if (examineTopic) {
            // examine topic exist.
            int count = retryCheckCount;
            while (count-- > 0) {
                if (this.topicExist(topic)) {
                    return true;
                }
            }
        } else {
            return true;
        }
        return false;
    }

    /**
     * Creates a subscription group configuration on all broker masters that serve the given sample topic.
     *
     * @param sampleTopic topic used to discover the target brokers via NameServer route lookup
     * @param config      subscription group configuration to create
     * @return {@code true} if the configuration was applied to at least one broker master
     */
    @Override
    public boolean createSubscriptionGroup(String sampleTopic, SubscriptionGroupConfig config) {
        return forEachBrokerMaster(sampleTopic, addr -> {
            try {
                brokerSubscriptionOps.createSubscriptionGroup(addr, config, Duration.ofSeconds(3).toMillis());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    /**
     * Deletes a subscription group on all broker masters that serve the given sample topic.
     *
     * @param sampleTopic topic used to discover the target brokers via NameServer route lookup
     * @param groupName   name of the consumer group to delete
     * @return {@code true} if the deletion was applied to at least one broker master
     */
    @Override
    public boolean deleteSubscriptionGroup(String sampleTopic, String groupName) {
        return forEachBrokerMaster(sampleTopic, addr -> {
            try {
                brokerSubscriptionOps.deleteSubscriptionGroup(addr, groupName, true, Duration.ofSeconds(3).toMillis());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    /**
     * Iterates over every master broker that serves the given sample topic and applies the given action.
     *
     * @param sampleTopic topic used to look up the route and discover broker masters
     * @param action      function to call for each master broker address; may throw {@link RuntimeException}
     *                    to skip a broker (the exception is logged and iteration continues)
     * @return {@code true} if the action succeeded on at least one broker master
     */
    private boolean forEachBrokerMaster(String sampleTopic, Function<String, Void> action) {
        TopicRouteData route;
        try {
            route = this.getTopicRouteDataDirectlyFromNameServer(sampleTopic);
        } catch (Exception e) {
            log.error("traffic-label admin: get route for {} failed.", sampleTopic, e);
            return false;
        }
        if (route == null || route.getBrokerDatas().isEmpty()) {
            return false;
        }
        boolean any = false;
        for (BrokerData brokerData : route.getBrokerDatas()) {
            String addr = brokerData.getBrokerAddrs() == null ? null
                : brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
            if (addr == null) {
                continue;
            }
            try {
                action.apply(addr);
                any = true;
            } catch (Exception e) {
                log.error("traffic-label admin: action on broker {} failed.", addr, e);
            }
        }
        return any;
    }

    protected TopicRouteData getTopicRouteDataDirectlyFromNameServer(String topic) throws Exception {
        return this.getClient().getTopicRouteInfoFromNameServer(topic, Duration.ofSeconds(3).toMillis());
    }

    protected MQClientAPIExt getClient() {
        return this.mqClientAPIFactory.getClient();
    }

    /**
     * Production implementation of {@link BrokerSubscriptionOps} that delegates directly to
     * the {@link MQClientAPIExt} instance returned by {@link #getClient()}.
     */
    private class DefaultBrokerSubscriptionOps implements BrokerSubscriptionOps {

        @Override
        public void createSubscriptionGroup(String brokerAddr, SubscriptionGroupConfig config,
            long timeoutMillis) throws Exception {
            getClient().createSubscriptionGroup(brokerAddr, config, timeoutMillis);
        }

        @Override
        public void deleteSubscriptionGroup(String brokerAddr, String groupName,
            boolean removeOffset, long timeoutMillis) throws Exception {
            getClient().deleteSubscriptionGroup(brokerAddr, groupName, removeOffset, timeoutMillis);
        }
    }
}
