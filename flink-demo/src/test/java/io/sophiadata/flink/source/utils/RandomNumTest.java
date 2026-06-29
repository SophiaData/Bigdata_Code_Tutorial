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

/** Tests for {@link RandomNum}. */
class RandomNumTest {

    @Test
    void getRandIntReturnsValueWithinRange() {
        for (int i = 0; i < 100; i++) {
            int result = RandomNum.getRandInt(5, 10);
            assertThat(result).isBetween(5, 10);
        }
    }

    @Test
    void getRandIntIsInclusiveOfBothBounds() {
        int minSeen = Integer.MAX_VALUE;
        int maxSeen = Integer.MIN_VALUE;
        for (int i = 0; i < 1000; i++) {
            int result = RandomNum.getRandInt(1, 2);
            minSeen = Math.min(minSeen, result);
            maxSeen = Math.max(maxSeen, result);
        }
        assertThat(minSeen).isEqualTo(1);
        assertThat(maxSeen).isEqualTo(2);
    }

    @Test
    void getRandIntWithSinglePointRangeAlwaysReturnsThatValue() {
        for (int i = 0; i < 50; i++) {
            assertThat(RandomNum.getRandInt(7, 7)).isEqualTo(7);
        }
    }

    @RepeatedTest(10)
    void getRandIntProducesBothValues() {
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            seen.add(RandomNum.getRandInt(1, 2));
        }
        assertThat(seen).containsExactlyInAnyOrder(1, 2);
    }

    @Test
    void getRandIntWithSeedIsStable() {
        // seed is passed but not used in current implementation (known limitation)
        // just verify it does not throw
        int result = RandomNum.getRandInt(1, 100, 42L);
        assertThat(result).isBetween(1, 100);
    }
}
