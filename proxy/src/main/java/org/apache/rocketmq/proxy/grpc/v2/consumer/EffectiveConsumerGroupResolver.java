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

import apache.rocketmq.v2.Settings;
import java.util.Objects;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;

/**
 * Resolves the consumer group bound to a gRPC client.
 *
 * <p>Once a client has registered, the group stored in its settings is authoritative. This keeps
 * request routing paired with registration even when later requests omit or change the traffic
 * label header.
 */
public class EffectiveConsumerGroupResolver {

    private final TrafficLabelRouter trafficLabelRouter;
    private final GrpcClientSettingsManager grpcClientSettingsManager;

    public EffectiveConsumerGroupResolver(TrafficLabelRouter trafficLabelRouter,
        GrpcClientSettingsManager grpcClientSettingsManager) {
        this.trafficLabelRouter = trafficLabelRouter;
        this.grpcClientSettingsManager = grpcClientSettingsManager;
    }

    /**
     * Resolves the group used to register a consumer, provisioning a gray group when no existing
     * client binding can be reused.
     */
    public String resolveForRegistration(ProxyContext ctx, String logicalGroup) {
        if (!isEnabled()) {
            return logicalGroup;
        }
        String registeredGroup = findRegisteredGroup(ctx, logicalGroup);
        if (registeredGroup != null) {
            return registeredGroup;
        }
        return trafficLabelRouter.rewriteRegistrationGroup(ctx, logicalGroup);
    }

    /**
     * Resolves the group used by a consumer request without registration side effects.
     */
    public String resolveForRequest(ProxyContext ctx, String logicalGroup) {
        if (!isEnabled()) {
            return logicalGroup;
        }
        String registeredGroup = findRegisteredGroup(ctx, logicalGroup);
        if (registeredGroup != null) {
            return registeredGroup;
        }
        return trafficLabelRouter.rewriteGroup(ctx, logicalGroup);
    }

    /**
     * Resolves a receive request using the group already bound to the client when available.
     */
    public LabelRoutingResolver.RoutingDecision resolveForReceive(ProxyContext ctx, String topic,
        String logicalGroup, String expression, String expressionType) {
        String effectiveGroup = resolveForRequest(ctx, logicalGroup);
        return trafficLabelRouter.resolveForReceive(
            ctx, topic, logicalGroup, effectiveGroup, expression, expressionType);
    }

    protected String findRegisteredGroup(ProxyContext ctx, String logicalGroup) {
        if (ctx == null || ctx.getClientID() == null) {
            return null;
        }
        Settings settings = grpcClientSettingsManager.getRawClientSettings(ctx.getClientID());
        if (settings == null || !settings.hasSubscription()) {
            return null;
        }
        String registeredGroup = settings.getSubscription().getGroup().getName();
        if (Objects.equals(registeredGroup, logicalGroup)
            || Objects.equals(TrafficLabel.parseLogicalGroup(registeredGroup), logicalGroup)) {
            return registeredGroup;
        }
        return null;
    }

    protected boolean isEnabled() {
        return ConfigurationManager.getProxyConfig().isEnableTrafficLabelRouting();
    }
}
