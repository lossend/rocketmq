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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;

/**
 * Facade for traffic-label-based consumer group routing.
 *
 * <p>All traffic-label routing decisions are gated by a master switch
 * ({@code enableTrafficLabelRouting} in {@link org.apache.rocketmq.proxy.config.ProxyConfig}).
 * When the switch is off every method returns the original value unchanged and
 * no side-effects (group creation) occur.
 *
 * <p>Typical call sites:
 * <ol>
 *   <li>Message-receive path — call {@link #resolveForReceive} to obtain the effective group
 *       and the SQL92 filter expression for the label-isolated queue.
 *   <li>Acknowledge / change-invisible path — call {@link #rewriteGroup} to translate the
 *       consumer group back to the virtual group name that owns the receipt handle.
 * </ol>
 */
public class TrafficLabelRouter {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final LabelRoutingResolver resolver;
    private final LabelGroupBootstrapper bootstrapper;

    /**
     * Creates a new {@code TrafficLabelRouter}.
     *
     * @param resolver     resolves a (group, label) pair into a {@link LabelRoutingResolver.RoutingDecision}
     * @param bootstrapper ensures a virtual consumer group and its subscriptions exist before use
     */
    public TrafficLabelRouter(LabelRoutingResolver resolver, LabelGroupBootstrapper bootstrapper) {
        this.resolver = resolver;
        this.bootstrapper = bootstrapper;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Rewrites the consumer group name for gray traffic.
     *
     * <p>When the master switch is disabled or the context carries no gray label,
     * {@code originGroup} is returned unchanged and the bootstrapper is never called.
     *
     * @param ctx         proxy context holding the optional traffic label
     * @param topic       topic being consumed (used for group bootstrap)
     * @param originGroup the original consumer group name supplied by the client
     * @return the effective group name — either the original or the virtual gray group
     */
    public String rewriteGroup(ProxyContext ctx, String topic, String originGroup) {
        if (!isEnabled()) {
            return originGroup;
        }
        String label = TrafficLabelExtractor.extract(ctx);
        if (!TrafficLabel.isGray(label)) {
            return originGroup;
        }
        String effectiveGroup = TrafficLabel.effectiveGroup(originGroup, label);
        bootstrapper.ensureGroup(topic, effectiveGroup);
        if (isLogEnabled()) {
            log.info("traffic-label rewrite group {} -> {} (label={})", originGroup, effectiveGroup, label);
        }
        return effectiveGroup;
    }

    /**
     * Resolves the full routing decision for a message-receive request.
     *
     * <p>Returns {@code null} when the master switch is disabled so callers can fall
     * through to standard (non-label-aware) receive logic without branching.
     *
     * @param ctx              proxy context holding the optional traffic label
     * @param topic            topic being consumed (used for group bootstrap)
     * @param originGroup      the original consumer group name supplied by the client
     * @param originExpression the original SQL92 / tag filter expression from the client, may be {@code null}
     * @return a {@link LabelRoutingResolver.RoutingDecision} when routing is enabled, {@code null} otherwise
     */
    public LabelRoutingResolver.RoutingDecision resolveForReceive(ProxyContext ctx, String topic,
        String originGroup, String originExpression) {
        if (!isEnabled()) {
            return null;
        }
        String label = TrafficLabelExtractor.extract(ctx);
        LabelRoutingResolver.RoutingDecision decision = resolver.resolve(originGroup, label, originExpression);
        if (TrafficLabel.isGray(label)) {
            bootstrapper.ensureGroup(topic, decision.getEffectiveGroup());
        }
        if (isLogEnabled()) {
            log.info("traffic-label receive group {} -> {} sql92={}",
                originGroup, decision.getEffectiveGroup(), decision.getSql92());
        }
        return decision;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private boolean isEnabled() {
        return ConfigurationManager.getProxyConfig().isEnableTrafficLabelRouting();
    }

    private boolean isLogEnabled() {
        return ConfigurationManager.getProxyConfig().isEnableTrafficLabelRoutingLog();
    }
}
