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

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

/** (@sophiadata) (@date 2023/8/2 11:18). */
public class RandomNumString {

    public static final String getRandNumString(
            int fromNum, int toNum, int count, String delimiter, boolean canRepeat) {
        String numString = "";
        if (canRepeat) {
            ArrayList<Integer> numList = new ArrayList<>();
            while (numList.size() < count) {
                numList.add(fromNum + new Random().nextInt(toNum - fromNum + 1));
            }
            numString = StringUtils.join(numList, delimiter);
        } else {
            HashSet<Integer> numSet = new HashSet<>();
            if (count <= (toNum - fromNum + 1) / 2) {
                while (numSet.size() < count) {
                    numSet.add(fromNum + new Random().nextInt(toNum - fromNum + 1));
                }
            } else {
                HashSet<Integer> exNumSet = new HashSet<>();
                while (exNumSet.size() < ((toNum - fromNum + 1) - count)) {
                    exNumSet.add(fromNum + new Random().nextInt(toNum - fromNum + 1));
                }

                for (int i = fromNum; i <= toNum; i++) {
                    if (!exNumSet.contains(i)) {
                        numSet.add(i);
                    }
                }
            }
            numString = StringUtils.join(numSet, delimiter);
        }
        return numString;
    }

    public static final String getRandNumString(
            int fromNum, int toNum, int count, String delimiter) {
        return getRandNumString(fromNum, toNum, count, delimiter, true);
    }

    public static void main(String[] args) {
        System.out.println(getRandNumString(1, 3, 4, ",", false));
    }
}
