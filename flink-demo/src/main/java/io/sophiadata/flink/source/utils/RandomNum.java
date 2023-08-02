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

import java.util.Random;

/** (@sophiadata) (@date 2023/8/2 11:17). */
public class RandomNum {
    public static final int getRandInt(int fromNum, int toNum) {

        return fromNum + new Random().nextInt(toNum - fromNum + 1);
    }

    public static final int getRandInt(int fromNum, int toNum, Long seed) {

        return fromNum + new Random(seed).nextInt(toNum - fromNum + 1);
    }
}
