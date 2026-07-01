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

package io.sophiadata.flink.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.sophiadata.flink.base.BaseCode;

import java.util.ArrayList;

/** (@SophiaData) (@date 2023/6/6 14:42). */
@SuppressWarnings("deprecation")
public class IncrementMapFunction extends BaseCode {

    /** Exposed as a named class so it can be unit-tested without running the full pipeline. */
    public static class IncrementMapper implements MapFunction<Long, Long> {
        @Override
        public Long map(final Long value) {
            return value + 1L;
        }
    }

    public static void main(final String[] args) throws Exception {
        new IncrementMapFunction().init(args, "MapFunction");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {

        final ArrayList<Long> testData = new ArrayList<>();
        testData.add(1L);
        testData.add(2L);
        final DataStreamSource<Long> testDataStream = env.fromCollection(testData);
        final SingleOutputStreamOperator<Long> map = testDataStream.map(new IncrementMapper());
        // Print sink registers itself as the terminal operator of the streaming topology.
        // DO NOT call env.execute() here — BaseCode.init() calls it once after handle() returns.
        // Calling it twice would clear the transformations list (Flink 1.20 default) and the
        // second call would fail with "No operators defined in streaming topology".
        map.print();
    }
}
