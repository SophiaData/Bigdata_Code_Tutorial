/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sophiadata.flink.source.utils;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RandomOptionGroup}. */
class RandomOptionGroupTest {

    // -------------------------------------------------------------------------
    // String constructor
    // -------------------------------------------------------------------------

    @Test
    void stringConstructorBuildsEqualWeightOptions() {
        RandomOptionGroup<String> group = new RandomOptionGroup<>("a", "b", "c");
        assertThat(group.totalWeight).isEqualTo(3);
        assertThat(group.optList).hasSize(3);
    }

    // -------------------------------------------------------------------------
    // Varargs RanOpt constructor
    // -------------------------------------------------------------------------

    @Test
    void ranOptConstructorAggregatesWeights() {
        RandomOptionGroup<String> group =
                new RandomOptionGroup<>(new RanOpt<>("low", 1), new RanOpt<>("high", 9));
        assertThat(group.totalWeight).isEqualTo(10);
        assertThat(group.optList).hasSize(10);
    }

    @Test
    void ranOptConstructorWithSingleOption() {
        RandomOptionGroup<String> group = new RandomOptionGroup<>(new RanOpt<>("only", 5));
        assertThat(group.totalWeight).isEqualTo(5);
    }

    // -------------------------------------------------------------------------
    // Boolean constructor (trueWeight / falseWeight)
    // -------------------------------------------------------------------------

    @Test
    void booleanConstructorRespectsWeights() {
        RandomOptionGroup<Boolean> group = new RandomOptionGroup<>(30, 70);
        assertThat(group.totalWeight).isEqualTo(100);
    }

    // -------------------------------------------------------------------------
    // String trueRate constructor
    // -------------------------------------------------------------------------

    @Test
    void stringRateConstructorDelegatesToCheckRatioNum() {
        RandomOptionGroup<Boolean> group = new RandomOptionGroup<>("25");
        assertThat(group.totalWeight).isEqualTo(100); // 25 + (100-25)
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    @Test
    void builderAggregatesWeightFromMultipleOptions() {
        RandomOptionGroup<String> group =
                RandomOptionGroup.<String>builder().add("a", 10).add("b", 20).add("c", 70).build();
        assertThat(group.totalWeight).isEqualTo(100);
        assertThat(group.optList).hasSize(100);
    }

    @Test
    void builderWithSingleOption() {
        RandomOptionGroup<String> group =
                RandomOptionGroup.<String>builder().add("only", 1).build();
        assertThat(group.totalWeight).isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // getValue (sampling)
    // -------------------------------------------------------------------------

    @RepeatedTest(10)
    void getValueReturnsOneOfTheOptions() {
        RanOpt<String>[] opts =
                new RanOpt[] {new RanOpt<>("x", 1), new RanOpt<>("y", 1), new RanOpt<>("z", 1)};
        RandomOptionGroup<String> group = new RandomOptionGroup<>(opts);
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            seen.add(group.getValue());
        }
        // With equal weights, all three values should appear in 100 samples (probabilistically)
        assertThat(seen).contains("x", "y", "z");
    }

    @Test
    void getValueWithSingleOptionAlwaysReturnsThatOption() {
        RandomOptionGroup<String> group =
                RandomOptionGroup.<String>builder().add("always", 1).build();
        for (int i = 0; i < 50; i++) {
            assertThat(group.getValue()).isEqualTo("always");
        }
    }

    // -------------------------------------------------------------------------
    // getRandomOpt
    // -------------------------------------------------------------------------

    @Test
    void getRandomOptReturnsRanOpt() {
        RandomOptionGroup<String> group =
                new RandomOptionGroup<>(new RanOpt<>("a", 1), new RanOpt<>("b", 1));
        RanOpt<String> opt = group.getRandomOpt();
        assertThat(opt.getValue()).isIn("a", "b");
    }

    // -------------------------------------------------------------------------
    // getRandStringValue
    // -------------------------------------------------------------------------

    @Test
    void getRandStringValueReturnsString() {
        RandomOptionGroup<String> group = new RandomOptionGroup<>("foo", "bar");
        String result = group.getRandStringValue();
        assertThat(result).isIn("foo", "bar");
    }

    // -------------------------------------------------------------------------
    // getRandIntValue
    // -------------------------------------------------------------------------

    @Test
    void getRandIntValueReturnsInteger() {
        RandomOptionGroup<Integer> group =
                RandomOptionGroup.<Integer>builder().add(10, 1).add(20, 1).build();
        Integer result = group.getRandIntValue();
        assertThat(result).isIn(10, 20);
    }

    // -------------------------------------------------------------------------
    // getRandBoolValue
    // -------------------------------------------------------------------------

    @Test
    void getRandBoolValueReturnsBoolean() {
        RandomOptionGroup<Boolean> group = new RandomOptionGroup<>(50, 50);
        Boolean result = group.getRandBoolValue();
        assertThat(result).isIn(Boolean.TRUE, Boolean.FALSE);
    }
}
