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
        String sql92Clause = toSql92Clause(originExpression, originExpressionType);
        String merged = mergeExpressions(sql92Clause, labelCondition);
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
     * Converts a consumer filter expression into a valid SQL-92 clause, or {@code null}
     * when no filtering is needed.
     *
     * <ul>
     *   <li>Blank or SUB_ALL ({@code "*"}) → {@code null} (no clause needed).</li>
     *   <li>TAG type → {@code TAGS in ('t1', 't2', ...)}; empty tag list → {@code null}.</li>
     *   <li>SQL-92 → passed through trimmed.</li>
     * </ul>
     *
     * @param expression     the filter expression string, may be {@code null}
     * @param expressionType the expression type ({@code "TAG"} or {@code "SQL92"}),
     *                       may be {@code null} (treated as TAG)
     * @return valid SQL-92 clause, or {@code null} to indicate "no origin filter"
     */
    private String toSql92Clause(String expression, String expressionType) {
        if (expression == null || expression.trim().isEmpty()
            || org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData.SUB_ALL.equals(
                expression.trim())) {
            return null;
        }
        if (org.apache.rocketmq.common.filter.ExpressionType.isTagType(expressionType)) {
            String[] parts = expression.split("\\|\\|");
            StringBuilder sb = new StringBuilder("TAGS in (");
            boolean first = true;
            for (String part : parts) {
                String tag = part.trim();
                if (tag.isEmpty()) {
                    continue;
                }
                if (!first) {
                    sb.append(", ");
                }
                // Escape single quotes inside tag values.
                sb.append('\'').append(tag.replace("'", "''")).append('\'');
                first = false;
            }
            if (first) {
                // All parts were empty after trimming — treat as no filter.
                return null;
            }
            sb.append(')');
            return sb.toString();
        }
        // SQL92 expression — pass through unchanged.
        return expression.trim();
    }

    /**
     * AND-merges an optional origin SQL-92 clause with the required label condition.
     * When {@code originClause} is absent the label condition is returned as-is;
     * otherwise both clauses are wrapped in parentheses and joined with {@code AND}.
     *
     * @param originClause   a valid SQL-92 clause derived from the origin expression,
     *                       may be {@code null} or blank
     * @param labelCondition the label filter condition
     * @return the merged SQL-92 expression
     */
    private String mergeExpressions(String originClause, String labelCondition) {
        if (originClause == null || originClause.trim().isEmpty()) {
            return labelCondition;
        }
        return "( " + originClause.trim() + " ) AND ( " + labelCondition + " )";
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
