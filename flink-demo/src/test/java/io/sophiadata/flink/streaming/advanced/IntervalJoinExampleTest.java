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

package io.sophiadata.flink.streaming.advanced;

import org.junit.jupiter.api.Test;

class IntervalJoinExampleTest {

    @Test
    void shouldClassExist() {
        // 验证类可以正常加载
        try {
            Class.forName("io.sophiadata.flink.streaming.advanced.IntervalJoinExample");
        } catch (final ClassNotFoundException e) {
            throw new AssertionError("IntervalJoinExample class not found", e);
        }
    }
}
