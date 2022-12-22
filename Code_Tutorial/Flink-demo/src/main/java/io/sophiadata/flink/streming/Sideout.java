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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import io.sophiadata.flink.base.BaseCode;

/** (@SophiaData) (@date 2022/10/29 19:12). */
public class Sideout extends BaseCode {

    public static void main(String[] args) throws Exception {
        //
        new Sideout().init(args, "sideout");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, ParameterTool params) {
        env.setParallelism(1);
        SingleOutputStreamOperator<String> sideout =
                env.fromElements(STREAM)
                        .flatMap(
                                (FlatMapFunction<String, String>)
                                        (value, out) -> {
                                            String[] splits = value.toLowerCase().split(",");
                                            for (String split : splits) {
                                                if (split.length() > 0) {
                                                    out.collect(split);
                                                }
                                            }
                                        })
                        .process(
                                new ProcessFunction<String, String>() {
                                    @Override
                                    public void processElement(
                                            String value,
                                            ProcessFunction<String, String>.Context ctx,
                                            Collector<String> out) {
                                        if (value.equals("hello")) {
                                            ctx.output(new OutputTag<String>("hello") {}, value);
                                        } else {
                                            out.collect(value);
                                        }
                                    }
                                })
                        .name("sideout");

        sideout.print("stream >>>  ").name("stream");
        sideout.getSideOutput(new OutputTag<String>("hello") {})
                .print(" hello stream >>> ")
                .name("hello");
    }

    private static final String[] STREAM =
            new String[] {"hello,nihao,nihao,world,bigdata,hadoop,hive,hive,hello,big"};
}
