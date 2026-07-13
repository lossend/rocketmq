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

import java.util.Set;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;

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
 *       and the SQL-92 filter expression for the label-isolated queue.</li>
 *   <li>Acknowledge / change-invisible path — call {@link #rewriteGroup} to translate the
 *       consumer group back to the virtual group name that owns the receipt handle.</li>
 *   <li>Consumer registration path — call {@link #rewriteRegistrationGroup} to rewrite the
 *       group for gray consumers before they register at the broker.</li>
 * </ol>
 */
public class TrafficLabelRouter {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final LabelRoutingResolver resolver;
    private final LabelGroupBootstrapper bootstrapper;
    private final TopicClientInfoIndex index;
    private final StandardFilterAssembler assembler;
    private final MetadataService metadataService;

    /**
     * Creates a router without a dynamic index (standard path returns {@code null}).
     * Used in contexts where index injection is not yet wired.
     *
     * @param resolver     resolves a (group, label) pair into a {@link LabelRoutingResolver.RoutingDecision}
     * @param bootstrapper ensures a virtual consumer group and its subscriptions exist before use
     */
    public TrafficLabelRouter(LabelRoutingResolver resolver, LabelGroupBootstrapper bootstrapper) {
        this(resolver, bootstrapper, null, null, null);
    }

    /**
     * Creates a fully-wired router with dynamic standard-side label filtering.
     *
     * @param resolver     resolves a (group, label) pair into a {@link LabelRoutingResolver.RoutingDecision}
     * @param bootstrapper ensures a virtual consumer group and its subscriptions exist before use
     * @param index        tracks which gray labels are currently online per (topic, logicalGroup)
     * @param assembler    assembles the dynamic SQL-92 exclusion filter for standard consumers
     */
    public TrafficLabelRouter(LabelRoutingResolver resolver, LabelGroupBootstrapper bootstrapper,
        TopicClientInfoIndex index, StandardFilterAssembler assembler) {
        this(resolver, bootstrapper, index, assembler, null);
    }

    /**
     * Creates a fully-wired router with dynamic standard-side label filtering and a consume-path
     * safety net backed by {@link MetadataService}.
     *
     * @param resolver        resolves a (group, label) pair into a {@link LabelRoutingResolver.RoutingDecision}
     * @param bootstrapper    ensures a virtual consumer group and its subscriptions exist before use
     * @param index           tracks which gray labels are currently online per (topic, logicalGroup)
     * @param assembler       assembles the dynamic SQL-92 exclusion filter for standard consumers
     * @param metadataService looks up whether the gray group is present in the metadata cache to gate
     *                        lazy re-provisioning; may be {@code null} to disable the safety net
     */
    public TrafficLabelRouter(LabelRoutingResolver resolver, LabelGroupBootstrapper bootstrapper,
        TopicClientInfoIndex index, StandardFilterAssembler assembler, MetadataService metadataService) {
        this.resolver = resolver;
        this.bootstrapper = bootstrapper;
        this.index = index;
        this.assembler = assembler;
        this.metadataService = metadataService;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Rewrites the consumer group name for gray traffic on the receive/ack paths.
     *
     * <p>When the master switch is disabled or the context carries no gray label,
     * {@code originGroup} is returned unchanged. Group creation is intentionally restricted to
     * the registration path.
     *
     * @param ctx         proxy context holding the optional traffic label
     * @param topic       topic being consumed
     * @param originGroup the original consumer group name supplied by the client
     * @return the effective group name — either the original or the virtual gray group
     */
    public String rewriteGroup(ProxyContext ctx, String topic, String originGroup) {
        return rewriteGroup(ctx, originGroup);
    }

    /**
     * Rewrites the consumer group name without registration side effects.
     *
     * @param ctx proxy context holding the optional traffic label
     * @param group original consumer group name
     * @return rewritten group or the original when no rewrite applies
     */
    public String rewriteGroup(ProxyContext ctx, String group) {
        if (!isEnabled()) {
            return group;
        }
        String label = TrafficLabelExtractor.extract(ctx);
        if (!TrafficLabel.isGray(label)) {
            return group;
        }
        String effectiveGroup = TrafficLabel.effectiveGroup(group, label);
        if (isLogEnabled()) {
            log.info("traffic-label rewrite group {} -> {} (label={})", group, effectiveGroup, label);
        }
        return effectiveGroup;
    }

    /**
     * Rewrites a registration group and creates the gray group before broker registration.
     */
    public String rewriteRegistrationGroup(ProxyContext ctx, String group) {
        String effectiveGroup = rewriteGroup(ctx, group);
        if (!effectiveGroup.equals(group)
            && !bootstrapper.ensureGrayGroupFromOrigin(group, effectiveGroup)) {
            throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR,
                "failed to create gray consumer group " + effectiveGroup + " from " + group);
        }
        return effectiveGroup;
    }

    /**
     * Resolves the full routing decision for a message-receive request.
     *
     * <ul>
     *   <li>Gray consumers: routes to the virtual group {@code originGroup%label} and appends a
     *       label-match condition to the origin filter.</li>
     *   <li>Standard consumers: keeps the origin group and appends a dynamic exclusion clause
     *       assembled from the set of currently-online gray labels. Returns {@code null} when no
     *       label filter is necessary (index empty, no origin clause).</li>
     * </ul>
     *
     * <p>Returns {@code null} when the master switch is disabled so callers can fall
     * through to standard (non-label-aware) receive logic without branching.
     *
     * @param ctx                  proxy context holding the optional traffic label
     * @param topic                topic being consumed
     * @param originGroup          the original consumer group name supplied by the client
     * @param originExpression     the original filter expression from the client (TAG string or
     *                             SQL-92), may be {@code null}
     * @param originExpressionType the expression type ({@code "TAG"} or {@code "SQL92"}),
     *                             may be {@code null} (treated as TAG)
     * @return a {@link LabelRoutingResolver.RoutingDecision} when routing is enabled and a filter
     *         change is needed, {@code null} otherwise
     */
    public LabelRoutingResolver.RoutingDecision resolveForReceive(ProxyContext ctx, String topic,
        String originGroup, String originExpression, String originExpressionType) {
        if (!isEnabled()) {
            return null;
        }
        String label = TrafficLabelExtractor.extract(ctx);
        if (TrafficLabel.isGray(label)) {
            return resolveGray(ctx, originGroup, label, originExpression, originExpressionType);
        }
        return resolveStandard(topic, originGroup, originExpression, originExpressionType);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Resolves the gray routing decision, lazily re-provisioning the gray group when it is missing
     * from the metadata cache (the consume-path safety net) before delegating to the resolver.
     *
     * @param ctx                  proxy context used for the metadata lookup
     * @param originGroup          the original consumer group name supplied by the client
     * @param label                the gray traffic label
     * @param originExpression     the original filter expression, may be {@code null}
     * @param originExpressionType the expression type, may be {@code null} (treated as TAG)
     * @return the gray {@link LabelRoutingResolver.RoutingDecision}
     */
    private LabelRoutingResolver.RoutingDecision resolveGray(ProxyContext ctx, String originGroup,
        String label, String originExpression, String originExpressionType) {
        String effectiveGroup = TrafficLabel.effectiveGroup(originGroup, label);
        // Cache-gated safety net: if the gray group is absent from the metadata cache,
        // attempt lazy provisioning (e.g. after cleaner deletion or a transient failure).
        if (metadataService != null
                && metadataService.getSubscriptionGroupConfig(ctx, effectiveGroup) == null) {
            bootstrapper.ensureGrayGroupFromOrigin(originGroup, effectiveGroup);
        }
        LabelRoutingResolver.RoutingDecision decision =
            resolver.resolve(originGroup, label, originExpression, originExpressionType);
        if (isLogEnabled()) {
            log.info("traffic-label receive group {} -> {} sql92={}",
                originGroup, decision.getEffectiveGroup(), decision.getSql92());
        }
        return decision;
    }

    private LabelRoutingResolver.RoutingDecision resolveStandard(String topic, String originGroup,
        String originExpression, String originExpressionType) {
        if (index == null || assembler == null) {
            return null;
        }
        Set<String> activeLabels = index.getActiveIsolatedLabels(topic, originGroup);
        String sql92 = assembler.assemble(topic, originGroup, originExpression, originExpressionType, activeLabels);
        if (sql92 == null) {
            return null;
        }
        if (isLogEnabled()) {
            log.info("traffic-label standard receive group {} sql92={}", originGroup, sql92);
        }
        return new LabelRoutingResolver.RoutingDecision(originGroup, sql92);
    }

    private boolean isEnabled() {
        return ConfigurationManager.getProxyConfig().isEnableTrafficLabelRouting();
    }

    private boolean isLogEnabled() {
        return ConfigurationManager.getProxyConfig().isEnableTrafficLabelRoutingLog();
    }
}
