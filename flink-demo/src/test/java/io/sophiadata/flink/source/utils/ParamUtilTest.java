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

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ParamUtil}. */
class ParamUtilTest {

    // -------------------------------------------------------------------------
    // checkRatioNum
    // -------------------------------------------------------------------------

    @Test
    void checkRatioNumAcceptsValidRange() {
        assertThat(ParamUtil.checkRatioNum("0")).isEqualTo(0);
        assertThat(ParamUtil.checkRatioNum("50")).isEqualTo(50);
        assertThat(ParamUtil.checkRatioNum("100")).isEqualTo(100);
    }

    @Test
    void checkRatioNumRejectsOutOfRange() {
        assertThatThrownBy(() -> ParamUtil.checkRatioNum("-1"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("比率");
        assertThatThrownBy(() -> ParamUtil.checkRatioNum("101"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("比率");
    }

    @Test
    void checkRatioNumRejectsNonNumeric() {
        assertThatThrownBy(() -> ParamUtil.checkRatioNum("abc"))
                .isInstanceOf(RuntimeException.class);
    }

    // -------------------------------------------------------------------------
    // checkDateTime
    // -------------------------------------------------------------------------

    @Test
    void checkDateTimeParsesValidDate() {
        LocalDateTime result = ParamUtil.checkDateTime("2024-01-15");
        assertThat(result).isNotNull();
        assertThat(result.getYear()).isEqualTo(2024);
        assertThat(result.getMonthValue()).isEqualTo(1);
        assertThat(result.getDayOfMonth()).isEqualTo(15);
    }

    @Test
    void checkDateTimeRejectsInvalidFormat() {
        assertThatThrownBy(() -> ParamUtil.checkDateTime("01-15-2024"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("日期");
        assertThatThrownBy(() -> ParamUtil.checkDateTime("not-a-date"))
                .isInstanceOf(RuntimeException.class);
    }

    // -------------------------------------------------------------------------
    // checkBoolean (note: only supports lowercase "true"/"false")
    // -------------------------------------------------------------------------

    @Test
    void checkBooleanAcceptsTrueValues() {
        assertThat(ParamUtil.checkBoolean("1")).isTrue();
        assertThat(ParamUtil.checkBoolean("true")).isTrue();
    }

    @Test
    void checkBooleanAcceptsFalseValues() {
        assertThat(ParamUtil.checkBoolean("0")).isFalse();
        assertThat(ParamUtil.checkBoolean("false")).isFalse();
    }

    @Test
    void checkBooleanRejectsInvalidValue() {
        assertThatThrownBy(() -> ParamUtil.checkBoolean("yes"))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> ParamUtil.checkBoolean("")).isInstanceOf(RuntimeException.class);
        // checkBoolean is case-sensitive, so uppercase values are rejected
        assertThatThrownBy(() -> ParamUtil.checkBoolean("TRUE"))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> ParamUtil.checkBoolean("FALSE"))
                .isInstanceOf(RuntimeException.class);
    }

    // -------------------------------------------------------------------------
    // checkRate
    // -------------------------------------------------------------------------

    @Test
    void checkRateParsesValidRates() {
        Integer[] rates = ParamUtil.checkRate("75:10:15", 3);
        assertThat(rates).containsExactly(75, 10, 15);
    }

    @Test
    void checkRateParsesTwoRates() {
        Integer[] rates = ParamUtil.checkRate("30:70", 2);
        assertThat(rates).containsExactly(30, 70);
    }

    @Test
    void checkRateRejectsMismatchedCount() {
        assertThatThrownBy(() -> ParamUtil.checkRate("10:20:30", 2))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> ParamUtil.checkRate("10", 3)).isInstanceOf(RuntimeException.class);
    }

    @Test
    void checkRateRejectsOutOfRangeValues() {
        assertThatThrownBy(() -> ParamUtil.checkRate("150:0", 2))
                .isInstanceOf(RuntimeException.class);
    }

    // -------------------------------------------------------------------------
    // checkArray
    // -------------------------------------------------------------------------

    @Test
    void checkArraySplitsCommaSeparatedValues() {
        assertThat(ParamUtil.checkArray("a,b,c")).containsExactly("a", "b", "c");
        assertThat(ParamUtil.checkArray("single")).containsExactly("single");
    }

    @Test
    void checkArrayRejectsNull() {
        assertThatThrownBy(() -> ParamUtil.checkArray(null))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("空");
    }

    // -------------------------------------------------------------------------
    // checkCount
    // -------------------------------------------------------------------------

    @Test
    void checkCountParsesValidNumber() {
        assertThat(ParamUtil.checkCount("42")).isEqualTo(42);
        assertThat(ParamUtil.checkCount("  7  ")).isEqualTo(7);
    }

    @Test
    void checkCountReturnsZeroForNull() {
        assertThat(ParamUtil.checkCount(null)).isEqualTo(0);
    }

    @Test
    void checkCountRejectsNonNumeric() {
        assertThatThrownBy(() -> ParamUtil.checkCount("nan")).isInstanceOf(RuntimeException.class);
    }
}
