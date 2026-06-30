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
public class ParamUtil {

    private static final Logger log = LoggerFactory.getLogger(ParamUtil.class);

    public static Integer checkRatioNum(String rate) {
        try {
            Integer rateNum = Integer.valueOf(rate);
            if (rateNum < 0 || rateNum > 100) {
                throw new RuntimeException("输入的比率必须为0 - 100 的数字");
            }
            return rateNum;
        } catch (Exception e) {
            throw new RuntimeException("输入的比率必须为0 - 100 的数字");
        }
    }

    /**
     * Parse a {@code yyyy-MM-dd} date string and combine it with the current local time, returning
     * the result as a {@link LocalDateTime}. Replaces the previous {@code Date}-based helper.
     */
    public static LocalDateTime checkDateTime(String dateString) {
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        try {
            LocalDate date = LocalDate.parse(dateString, dateFormatter);
            LocalDateTime combined = LocalDateTime.of(date, LocalTime.now());
            log.debug("parsed {} -> {}", dateString, combined.format(datetimeFormatter));
            return combined;
        } catch (Exception e) {
            throw new RuntimeException("必须为日期型格式 例如： 2020-02-02");
        }
    }

    public static Boolean checkBoolean(String bool) {
        if (bool.equals("1") || bool.equals("true")) {
            return true;
        } else if (bool.equals("0") || bool.equals("false")) {
            return false;
        } else {
            throw new RuntimeException("是非型参数请填写：1或0 ， true 或 false");
        }
    }

    public static Integer[] checkRate(String rateString, int rateCount) {
        try {
            String[] rateArray = rateString.split(":");
            if (rateArray.length != rateCount) {
                throw new RuntimeException("请按比例个数不足 ");
            }
            Integer[] rateNumArr = new Integer[rateArray.length];
            for (int i = 0; i < rateArray.length; i++) {
                Integer rate = checkRatioNum(rateArray[i]);
                rateNumArr[i] = rate;
            }
            return rateNumArr;
        } catch (Exception e) {
            throw new RuntimeException("请按比例填写 如   75:10:15");
        }
    }

    public static String[] checkArray(String str) {

        if (str == null) {
            throw new RuntimeException("搜索词为空");
        }

        String[] split = str.split(",");
        return split;
    }

    public static Integer checkCount(String count) {
        try {
            if (count == null) {
                return 0;
            }
            Integer rateNum = Integer.valueOf(count.trim());
            return rateNum;
        } catch (Exception e) {
            throw new RuntimeException("输入的数据必须为数字");
        }
    }

    public static void main(String[] args) {
        System.out.println(ParamUtil.checkDateTime("2024-01-15"));
        System.out.println("ok");
    }
}
