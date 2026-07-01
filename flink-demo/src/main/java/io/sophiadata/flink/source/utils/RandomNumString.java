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
public final class RandomNumString {

    private RandomNumString() {}

    public static String getRandNumString(
            final int fromNum,
            final int toNum,
            final int count,
            final String delimiter,
            final boolean canRepeat) {
        if (canRepeat) {
            return buildWithRepeat(fromNum, toNum, count, delimiter);
        }
        return buildWithoutRepeat(fromNum, toNum, count, delimiter);
    }

    private static String buildWithRepeat(
            final int fromNum, final int toNum, final int count, final String delimiter) {
        final ArrayList<Integer> numList = new ArrayList<>();
        while (numList.size() < count) {
            numList.add(fromNum + new Random().nextInt(toNum - fromNum + 1));
        }
        return StringUtils.join(numList, delimiter);
    }

    private static String buildWithoutRepeat(
            final int fromNum, final int toNum, final int count, final String delimiter) {
        final int range = toNum - fromNum + 1;
        final HashSet<Integer> numSet = new HashSet<>();
        if (count <= range / 2) {
            while (numSet.size() < count) {
                numSet.add(fromNum + new Random().nextInt(range));
            }
        } else {
            final HashSet<Integer> exNumSet = new HashSet<>();
            while (exNumSet.size() < (range - count)) {
                exNumSet.add(fromNum + new Random().nextInt(range));
            }
            for (int i = fromNum; i <= toNum; i++) {
                if (!exNumSet.contains(i)) {
                    numSet.add(i);
                }
            }
        }
        return StringUtils.join(numSet, delimiter);
    }

    public static String getRandNumString(
            final int fromNum, final int toNum, final int count, final String delimiter) {
        return getRandNumString(fromNum, toNum, count, delimiter, true);
    }

    public static void main(final String[] args) {
        System.out.println(getRandNumString(1, 3, 4, ",", false));
    }
}
