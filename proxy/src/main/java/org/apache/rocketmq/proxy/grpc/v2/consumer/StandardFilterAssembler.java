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
import java.util.TreeSet;

/**
 * Assembles the SQL-92 filter expression for standard (non-gray) consumers.
 *
 * <p>When gray-lane consumers are online for a given {@code (topic, logicalGroup)}, the standard
 * consumer must exclude those labels so standard messages are not routed to gray virtual groups.
 * The exclusion is AND-merged with any origin filter the consumer already carries.
 *
 * <p>This class is stateless; a single instance may be shared across all receive threads.
 */
public class StandardFilterAssembler {

    /**
     * Builds the composite SQL-92 filter for a standard consumer receive request.
     *
     * <p>When {@code activeLabels} is empty and {@code originExpression} produces no clause,
     * returns {@code null} to signal "no change needed". Otherwise:
     * <ul>
     *   <li>If active labels are present, builds an exclusion clause of the form
     *       {@code __SERVICE_TAG__ IS NULL OR (__SERVICE_TAG__ <> 'g1' AND ...)}
     *       with labels sorted alphabetically for determinism.</li>
     *   <li>AND-merges the exclusion with the converted origin expression via
     *       {@link Sql92Filters#merge}.</li>
     * </ul>
     *
     * @param topic                the topic being subscribed to
     * @param logicalGroup         the logical consumer group
     * @param originExpression     the origin filter expression from the consumer, may be {@code null}
     * @param originExpressionType the expression type ({@code "TAG"} or {@code "SQL92"}), may be {@code null}
     * @param activeLabels         currently-online isolated labels for this {@code (topic, logicalGroup)},
     *                             may be {@code null} or empty
     * @return the assembled SQL-92 filter expression, or {@code null} when no filtering is needed
     */
    public String assemble(String topic, String logicalGroup,
        String originExpression, String originExpressionType,
        Set<String> activeLabels) {

        String originClause = Sql92Filters.toSql92Clause(originExpression, originExpressionType);

        if (activeLabels == null || activeLabels.isEmpty()) {
            return originClause;
        }

        // Sort labels for deterministic output regardless of map iteration order.
        Set<String> sorted = new TreeSet<>(activeLabels);
        StringBuilder exclusion = new StringBuilder(TrafficLabel.PROPERTY_KEY).append(" IS NULL OR (");
        boolean first = true;
        for (String label : sorted) {
            if (!first) {
                exclusion.append(" AND ");
            }
            exclusion.append(TrafficLabel.PROPERTY_KEY)
                .append(" <> '")
                .append(label.replace("'", "''"))
                .append('\'');
            first = false;
        }
        exclusion.append(')');

        return Sql92Filters.merge(originClause, exclusion.toString());
    }
}
