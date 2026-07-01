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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import io.sophiadata.flink.base.BaseCode;

/** (@SophiaData) (@date 2022/10/29 13:50). */
@SuppressWarnings("deprecation")
public class WordCount extends BaseCode {

    public static void main(final String[] args) throws Exception {
        //
        new WordCount().init(args, "WordCount");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        final DataStream<String> source = env.fromElements(WORDS);
        final DataStream<Tuple2<String, Integer>> flatMapped =
                source.flatMap(
                        new FlatMapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(
                                    final String value,
                                    final Collector<Tuple2<String, Integer>> out)
                                    throws Exception {
                                for (final String word : value.toLowerCase().split(",")) {
                                    if (!word.isEmpty()) {
                                        out.collect(new Tuple2<>(word, 1));
                                    }
                                }
                            }
                        });
        flatMapped
                .keyBy(value -> value.f0)
                .reduce(
                        (ReduceFunction<Tuple2<String, Integer>>)
                                (value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + value2.f1))
                .print()
                .setParallelism(1)
                .name("WordCount_Print");
    }

    private static final String[] WORDS =
            new String[] {"hello,nihao,nihao,world,bigdata,hadoop,hive,hive,hello,big"};
}
