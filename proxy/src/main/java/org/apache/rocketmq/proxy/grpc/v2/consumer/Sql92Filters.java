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

import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * SQL-92 filter helpers shared by the traffic-label routing components.
 *
 * <p>Both {@link LabelRoutingResolver} (gray path) and the standard-side filter
 * assembler need to convert a caller-supplied filter expression (TAG or SQL-92)
 * into a valid SQL-92 clause and AND-merge it with a label condition. Keeping the
 * logic here guarantees both paths behave identically.
 */
public final class Sql92Filters {

    private Sql92Filters() {}

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
    public static String toSql92Clause(String expression, String expressionType) {
        if (expression == null || expression.trim().isEmpty()
            || SubscriptionData.SUB_ALL.equals(expression.trim())) {
            return null;
        }
        if (ExpressionType.isTagType(expressionType)) {
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
     * AND-merges an optional origin SQL-92 clause with a required condition.
     * When {@code originClause} is absent the condition is returned as-is;
     * otherwise both clauses are wrapped in parentheses and joined with {@code AND}.
     *
     * @param originClause a valid SQL-92 clause derived from the origin expression,
     *                     may be {@code null} or blank
     * @param condition    the condition to merge with
     * @return the merged SQL-92 expression
     */
    public static String merge(String originClause, String condition) {
        if (originClause == null || originClause.trim().isEmpty()) {
            return condition;
        }
        return "( " + originClause.trim() + " ) AND ( " + condition + " )";
    }
}
