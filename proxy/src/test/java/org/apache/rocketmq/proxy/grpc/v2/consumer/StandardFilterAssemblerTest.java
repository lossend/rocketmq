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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StandardFilterAssemblerTest {

    private StandardFilterAssembler assembler;

    @Before
    public void setUp() {
        assembler = new StandardFilterAssembler();
    }

    @Test
    public void activeLabels_noOrigin_returnsExclusionOnly() {
        Set<String> labels = setOf("gray2", "gray1"); // deliberately unsorted
        String result = assembler.assemble("T", "G", null, null, labels);
        // Labels must be sorted: gray1 before gray2
        assertThat(result).isEqualTo(
            "__SERVICE_TAG__ IS NULL OR (__SERVICE_TAG__ <> 'gray1' AND __SERVICE_TAG__ <> 'gray2')");
    }

    @Test
    public void activeLabels_withTagOrigin_mergesCorrectly() {
        Set<String> labels = setOf("gray1");
        String result = assembler.assemble("T", "G", "TagA", ExpressionType.TAG, labels);
        assertThat(result).isEqualTo(
            "( TAGS in ('TagA') ) AND ( __SERVICE_TAG__ IS NULL OR (__SERVICE_TAG__ <> 'gray1') )");
    }

    @Test
    public void activeLabels_withSql92Origin_mergesCorrectly() {
        Set<String> labels = setOf("gray1");
        String result = assembler.assemble("T", "G", "a > 1", ExpressionType.SQL92, labels);
        assertThat(result).isEqualTo(
            "( a > 1 ) AND ( __SERVICE_TAG__ IS NULL OR (__SERVICE_TAG__ <> 'gray1') )");
    }

    @Test
    public void emptyActiveLabels_noOrigin_returnsNull() {
        String result = assembler.assemble("T", "G", null, null, Collections.emptySet());
        assertThat(result).isNull();
    }

    @Test
    public void emptyActiveLabels_withSql92Origin_returnsOriginOnly() {
        String result = assembler.assemble("T", "G", "a > 1", ExpressionType.SQL92,
            Collections.emptySet());
        assertThat(result).isEqualTo("a > 1");
    }

    @Test
    public void singleQuoteInLabel_isEscaped() {
        Set<String> labels = setOf("gray'1");
        String result = assembler.assemble("T", "G", null, null, labels);
        assertThat(result).isEqualTo(
            "__SERVICE_TAG__ IS NULL OR (__SERVICE_TAG__ <> 'gray''1')");
    }

    @Test
    public void nullActiveLabels_noOrigin_returnsNull() {
        String result = assembler.assemble("T", "G", null, null, null);
        assertThat(result).isNull();
    }

    @Test
    public void nullActiveLabels_withOrigin_returnsOriginOnly() {
        String result = assembler.assemble("T", "G", "x = 1", ExpressionType.SQL92, null);
        assertThat(result).isEqualTo("x = 1");
    }

    /** Helper: ordered insertion set so we can pass labels in unsorted order and verify sorting. */
    private static Set<String> setOf(String... labels) {
        Set<String> set = new LinkedHashSet<>();
        for (String l : labels) {
            set.add(l);
        }
        return set;
    }
}
