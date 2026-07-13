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

import java.util.List;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public interface AdminService {

    boolean topicExist(String topic);

    boolean createTopicOnTopicBrokerIfNotExist(String createTopic, String sampleTopic, int wQueueNum,
        int rQueueNum, boolean examineTopic, int retryCheckCount);

    boolean createTopicOnBroker(String topic, int wQueueNum, int rQueueNum, List<BrokerData> curBrokerDataList,
        List<BrokerData> sampleBrokerDataList, boolean examineTopic, int retryCheckCount) throws Exception;

    /**
     * Creates a subscription group configuration on all broker masters that serve the given sample topic.
     *
     * @param sampleTopic topic used to discover the target brokers via NameServer route lookup
     * @param config      subscription group configuration to create
     * @return {@code true} if the configuration was applied to at least one broker master
     */
    boolean createSubscriptionGroup(String sampleTopic, SubscriptionGroupConfig config);

    /**
     * Clones a source subscription group to a target group on <em>all</em> cluster masters,
     * sourcing the configuration from the first master that hosts the source group. This ensures
     * the target group is reachable regardless of which master the proxy's random-broker lookup
     * selects. Existing target groups are left untouched.
     *
     * @param sourceGroup source consumer group name
     * @param targetGroup target consumer group name
     * @return {@code true} when the source group was found and the target group was confirmed
     *         (created or already present) on every cluster master; {@code false} if the source
     *         group exists on no master, or any master's create call failed
     */
    boolean cloneSubscriptionGroupIfAbsent(String sourceGroup, String targetGroup);

    /**
     * Deletes a subscription group on all broker masters that serve the given sample topic.
     *
     * @param sampleTopic topic used to discover the target brokers via NameServer route lookup
     * @param groupName   name of the consumer group to delete
     * @return {@code true} if the deletion was applied to at least one broker master
     */
    boolean deleteSubscriptionGroup(String sampleTopic, String groupName);
}
