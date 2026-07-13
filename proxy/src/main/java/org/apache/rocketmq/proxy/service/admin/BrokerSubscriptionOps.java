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

import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;

/**
 * Thin broker-level operations for subscription-group management.
 *
 * <p>Extracted as a package-private interface so that {@link DefaultAdminService} can be unit-
 * tested without constructing a real {@link org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt}
 * (which would start a Netty client).  The only production implementation delegates to
 * {@code MQClientAPIExt}.
 */
interface BrokerSubscriptionOps {

    /**
     * Returns all subscription-group configurations from one broker master.
     *
     * @param brokerAddr broker master address
     * @param timeoutMillis RPC timeout in milliseconds
     * @throws Exception on remoting / broker error
     */
    SubscriptionGroupWrapper getAllSubscriptionGroup(String brokerAddr, long timeoutMillis) throws Exception;

    /**
     * Creates (or updates) a subscription group on the broker at {@code brokerAddr}.
     *
     * @param brokerAddr    broker master address
     * @param config        subscription group configuration
     * @param timeoutMillis RPC timeout in milliseconds
     * @throws Exception on remoting / broker error
     */
    void createSubscriptionGroup(String brokerAddr, SubscriptionGroupConfig config,
        long timeoutMillis) throws Exception;

    /**
     * Deletes a subscription group on the broker at {@code brokerAddr}.
     *
     * @param brokerAddr    broker master address
     * @param groupName     consumer group name
     * @param removeOffset  whether to clean up the committed offset as well
     * @param timeoutMillis RPC timeout in milliseconds
     * @throws Exception on remoting / broker error
     */
    void deleteSubscriptionGroup(String brokerAddr, String groupName,
        boolean removeOffset, long timeoutMillis) throws Exception;
}
