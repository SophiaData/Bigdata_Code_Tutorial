/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sophiadata.flink.streming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.sophiadata.flink.base.BaseCode;

/** (@SophiaData) (@date 2022/10/29 13:50). */
public class WordCount extends BaseCode {

    public static void main(String[] args) throws Exception {
        //
        new WordCount().init(args, "WordCount");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, ParameterTool params) {
        env.fromElements(WORDS)
                .flatMap(
                        (FlatMapFunction<String, Tuple2<String, Integer>>)
                                (value, out) -> {
                                    String[] splits = value.toLowerCase().split(",");

                                    for (String split : splits) {
                                        if (split.length() > 0) {
                                            out.collect(new Tuple2<>(split, 1));
                                        }
                                    }
                                })
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
