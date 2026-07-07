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

/**
 * Resolves the effective consumer-group and SQL-92 filter expression for a subscribe
 * request based on the consumer's traffic label.
 *
 * <p>Gray consumers are routed to a virtual group ({@code originGroup%label}) and
 * receive only messages tagged with their label. Standard consumers stay on the
 * origin group and receive only messages that carry no label or the
 * {@value TrafficLabel#STANDARD} label. An optional caller-supplied filter expression
 * (TAG or SQL-92) is converted to a valid SQL-92 clause and AND-merged with the
 * label condition when present.
 */
public class LabelRoutingResolver {

    /**
     * Resolves the routing decision for a consumer.
     *
     * <p>The origin expression is converted to a valid SQL-92 clause before
     * being merged with the label condition:
     * <ul>
     *   <li>SUB_ALL ({@code "*"}) and blank expressions are dropped — the resulting SQL-92 is
     *       the label condition alone, which is what the broker already requires.</li>
     *   <li>TAG expressions (e.g. {@code "TagA || TagB"}) are converted to
     *       {@code TAGS in ('TagA', 'TagB')} — the RocketMQ SQL-92 reserved property for tags.
     *       This avoids passing raw tag syntax ({@code ||}) or the wildcard {@code *} to the
     *       broker's SQL-92 parser, which rejects them.</li>
     *   <li>SQL-92 expressions are passed through unchanged.</li>
     * </ul>
     *
     * @param originGroup          the original consumer group name
     * @param label                the traffic label extracted from the consumer metadata,
     *                             may be {@code null}
     * @param originExpression     the filter expression from the consumer (TAG string or SQL-92),
     *                             may be {@code null}
     * @param originExpressionType the expression type ({@code "TAG"} or {@code "SQL92"}),
     *                             may be {@code null} (treated as TAG)
     * @return a {@link RoutingDecision} containing the effective group and merged SQL-92 filter
     */
    public RoutingDecision resolve(String originGroup, String label,
        String originExpression, String originExpressionType) {
        String effectiveGroup = TrafficLabel.effectiveGroup(originGroup, label);
        String labelCondition = buildLabelCondition(label);
        String sql92Clause = Sql92Filters.toSql92Clause(originExpression, originExpressionType);
        String merged = Sql92Filters.merge(sql92Clause, labelCondition);
        return new RoutingDecision(effectiveGroup, merged);
    }

    /**
     * Builds the SQL-92 condition that matches only messages belonging to the given lane.
     *
     * @param label the traffic label, may be {@code null}
     * @return the SQL-92 label condition string
     */
    private String buildLabelCondition(String label) {
        if (TrafficLabel.isGray(label)) {
            return TrafficLabel.PROPERTY_KEY + " = '" + label + "'";
        }
        return TrafficLabel.PROPERTY_KEY + " IS NULL OR "
            + TrafficLabel.PROPERTY_KEY + " = '" + TrafficLabel.STANDARD + "'";
    }

    /**
     * Immutable result of a routing resolution.
     */
    public static class RoutingDecision {

        private final String effectiveGroup;
        private final String sql92;

        /**
         * Creates a new routing decision.
         *
         * @param effectiveGroup the consumer group to subscribe to
         * @param sql92          the SQL-92 filter expression
         */
        public RoutingDecision(String effectiveGroup, String sql92) {
            this.effectiveGroup = effectiveGroup;
            this.sql92 = sql92;
        }

        /**
         * Returns the effective consumer group name.
         *
         * @return effective consumer group
         */
        public String getEffectiveGroup() {
            return effectiveGroup;
        }

        /**
         * Returns the merged SQL-92 filter expression.
         *
         * @return SQL-92 expression
         */
        public String getSql92() {
            return sql92;
        }
    }
}
