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

import lombok.extern.slf4j.Slf4j;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/** (@sophiadata) (@date 2023/8/2 11:15). */
@Slf4j
public class ParamUtil {

    public static final Integer checkRatioNum(String rate) {
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

    public static final Date checkDate(String dateString) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
        SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            String timeString = timeFormat.format(new Date());
            String datetimeString = dateString + " " + timeString;
            Date date = datetimeFormat.parse(datetimeString);
            return date;
        } catch (ParseException e) {
            throw new RuntimeException("必须为日期型格式 例如： 2020-02-02");
        }
    }

    public static final Boolean checkBoolean(String bool) {
        if (bool.equals("1") || bool.equals("true")) {
            return true;
        } else if (bool.equals("0") || bool.equals("false")) {
            return false;
        } else {
            throw new RuntimeException("是非型参数请填写：1或0 ， true 或 false");
        }
    }

    public static final Integer[] checkRate(String rateString, int rateCount) {
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

    public static final String[] checkArray(String str) {

        if (str == null) {
            throw new RuntimeException("搜索词为空");
        }

        String[] split = str.split(",");
        return split;
    }

    public static final Integer checkCount(String count) {
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
        System.out.println(ParamUtil.checkDate("2019-13-1123"));
        ;
        System.out.println("ok");
    }
}
