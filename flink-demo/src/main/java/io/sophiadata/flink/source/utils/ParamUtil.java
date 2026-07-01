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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/** (@sophiadata) (@date 2023/8/2 11:15). */
public final class ParamUtil {

    private ParamUtil() {}

    private static final Logger LOG = LoggerFactory.getLogger(ParamUtil.class);

    public static Integer checkRatioNum(final String rate) {
        try {
            final Integer rateNum = Integer.valueOf(rate);
            if (rateNum < 0 || rateNum > 100) {
                throw new RuntimeException("输入的比率必须为0 - 100 的数字");
            }
            return rateNum;
        } catch (Exception e) {
            throw new RuntimeException("输入的比率必须为0 - 100 的数字", e);
        }
    }

    /**
     * Parse a {@code yyyy-MM-dd} date string and combine it with the current local time, returning
     * the result as a {@link LocalDateTime}. Replaces the previous {@code Date}-based helper.
     */
    public static LocalDateTime checkDateTime(final String dateString) {
        final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        final DateTimeFormatter datetimeFormatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        try {
            final LocalDate date = LocalDate.parse(dateString, dateFormatter);
            final LocalDateTime combined = LocalDateTime.of(date, LocalTime.now());
            LOG.debug("parsed {} -> {}", dateString, combined.format(datetimeFormatter));
            return combined;
        } catch (Exception e) {
            throw new RuntimeException("必须为日期型格式 例如： 2020-02-02", e);
        }
    }

    public static Boolean checkBoolean(final String bool) {
        if (bool.equals("1") || bool.equals("true")) {
            return true;
        } else if (bool.equals("0") || bool.equals("false")) {
            return false;
        } else {
            throw new RuntimeException("是非型参数请填写：1或0 ， true 或 false");
        }
    }

    public static Integer[] checkRate(final String rateString, final int rateCount) {
        try {
            final String[] rateArray = rateString.split(":");
            if (rateArray.length != rateCount) {
                throw new RuntimeException("请按比例个数不足 ");
            }
            Integer[] rateNumArr = new Integer[rateArray.length];
            for (int i = 0; i < rateArray.length; i++) {
                final Integer rate = checkRatioNum(rateArray[i]);
                rateNumArr[i] = rate;
            }
            return rateNumArr;
        } catch (Exception e) {
            throw new RuntimeException("请按比例填写 如   75:10:15", e);
        }
    }

    public static String[] checkArray(final String str) {

        if (str == null) {
            throw new RuntimeException("搜索词为空");
        }

        final String[] split = str.split(",");
        return split;
    }

    public static Integer checkCount(final String count) {
        try {
            if (count == null) {
                return 0;
            }
            final Integer rateNum = Integer.valueOf(count.trim());
            return rateNum;
        } catch (Exception e) {
            throw new RuntimeException("输入的数据必须为数字", e);
        }
    }

    public static void main(final String[] args) {
        System.out.println(ParamUtil.checkDateTime("2024-01-15"));
        System.out.println("ok");
    }
}
