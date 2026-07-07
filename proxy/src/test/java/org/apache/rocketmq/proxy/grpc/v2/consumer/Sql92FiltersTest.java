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
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class Sql92FiltersTest {

    // -----------------------------------------------------------------------
    // toSql92Clause
    // -----------------------------------------------------------------------

    @Test
    public void tag_expression_converted_to_TAGS_in() {
        assertThat(Sql92Filters.toSql92Clause("TagA || TagB", ExpressionType.TAG))
            .isEqualTo("TAGS in ('TagA', 'TagB')");
    }

    @Test
    public void single_tag_converted_to_TAGS_in() {
        assertThat(Sql92Filters.toSql92Clause("TagA", ExpressionType.TAG))
            .isEqualTo("TAGS in ('TagA')");
    }

    @Test
    public void null_expression_yields_null() {
        assertThat(Sql92Filters.toSql92Clause(null, ExpressionType.TAG)).isNull();
    }

    @Test
    public void blank_expression_yields_null() {
        assertThat(Sql92Filters.toSql92Clause("   ", ExpressionType.TAG)).isNull();
    }

    @Test
    public void sub_all_star_expression_yields_null() {
        assertThat(Sql92Filters.toSql92Clause("*", ExpressionType.TAG)).isNull();
    }

    @Test
    public void empty_tag_list_after_trimming_yields_null() {
        assertThat(Sql92Filters.toSql92Clause(" || ", ExpressionType.TAG)).isNull();
    }

    @Test
    public void sql92_expression_passed_through_trimmed() {
        assertThat(Sql92Filters.toSql92Clause("  a > 1  ", ExpressionType.SQL92))
            .isEqualTo("a > 1");
    }

    @Test
    public void single_quotes_in_tag_values_are_escaped() {
        assertThat(Sql92Filters.toSql92Clause("Tag'A", ExpressionType.TAG))
            .isEqualTo("TAGS in ('Tag''A')");
    }

    // -----------------------------------------------------------------------
    // merge
    // -----------------------------------------------------------------------

    @Test
    public void merge_null_origin_returns_label_condition() {
        assertThat(Sql92Filters.merge(null, "X")).isEqualTo("X");
    }

    @Test
    public void merge_blank_origin_returns_label_condition() {
        assertThat(Sql92Filters.merge("   ", "X")).isEqualTo("X");
    }

    @Test
    public void merge_two_clauses_wraps_and_joins_with_AND() {
        assertThat(Sql92Filters.merge("A", "B")).isEqualTo("( A ) AND ( B )");
    }
}
