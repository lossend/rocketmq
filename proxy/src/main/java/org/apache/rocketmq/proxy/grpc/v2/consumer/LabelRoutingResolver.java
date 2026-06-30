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
 * Stub placeholder — full implementation in Task 3.
 *
 * @author yangjie.sun
 */
public class LabelRoutingResolver {

    /**
     * Routing decision returned by {@link #resolve}.
     */
    public static class RoutingDecision {
        private final String effectiveGroup;
        private final String sql92;

        /**
         * Creates a routing decision.
         *
         * @param effectiveGroup the consumer group to subscribe to
         * @param sql92          the SQL-92 filter expression, or {@code null} if none
         */
        public RoutingDecision(String effectiveGroup, String sql92) {
            this.effectiveGroup = effectiveGroup;
            this.sql92 = sql92;
        }

        /**
         * Returns the effective consumer group.
         *
         * @return effective consumer group
         */
        public String getEffectiveGroup() {
            return effectiveGroup;
        }

        /**
         * Returns the SQL-92 filter expression.
         *
         * @return SQL-92 expression
         */
        public String getSql92() {
            return sql92;
        }
    }

    /**
     * Resolves routing for a consumer given its group, traffic label, and optional origin
     * SQL-92 expression. Stub implementation — always returns origin group with no filter.
     *
     * @param originGroup    the original consumer group
     * @param label          the traffic label (may be {@code null})
     * @param originSql92    optional caller-supplied SQL-92 expression (may be {@code null})
     * @return a {@link RoutingDecision}
     */
    public RoutingDecision resolve(String originGroup, String label, String originSql92) {
        // TODO: implement in Task 3
        return new RoutingDecision(originGroup, originSql92);
    }
}
