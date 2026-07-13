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

package org.apache.rocketmq.proxy.service.metadata;

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public interface MetadataService {

    TopicMessageType getTopicMessageType(ProxyContext ctx, String topic);

    SubscriptionGroupConfig getSubscriptionGroupConfig(ProxyContext ctx, String group);

    /**
     * Removes any cached entry for the given group so the next lookup fetches a fresh value.
     * Callers should invoke this after explicitly creating a subscription group to avoid a stale
     * negative cache entry masking the new group.
     *
     * @param group the consumer group name whose cache entry to invalidate
     */
    void invalidateSubscriptionGroupConfig(String group);

    CompletableFuture<User> getUser(ProxyContext ctx, String username);

    CompletableFuture<Acl> getAcl(ProxyContext ctx, Subject subject);
}
