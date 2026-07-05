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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.sophiadata.flink.base.BaseCode;

/**
 * 死信队列示例 —— 演示错误处理和失败重试模式。
 *
 * <p>场景：解析用户输入，无效数据进入死信流，有效数据正常处理。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.streaming.advanced.DeadLetterQueueExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>Side Output 实现死信队列
 *   <li>数据验证和错误分类
 *   <li>失败记录的隔离处理
 * </ul>
 */
public class DeadLetterQueueExample extends BaseCode {

    public static void main(final String[] args) throws Exception {
        new DeadLetterQueueExample().init(args, "Dead_Letter_Queue");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        // 模拟混合数据流：有效和无效数据
        final DataStream<String> stream =
                env.fromElements(
                        "100,Alice,alice@test.com",
                        "invalid_data",
                        "200,Bob,bob@test.com",
                        ",,",
                        "300,Charlie,charlie@test.com",
                        "abc,def,ghi");

        // 1. 解析数据
        final SingleOutputStreamOperator<Tuple2<Boolean, String>> parsed =
                stream.map(
                        new MapFunction<String, Tuple2<Boolean, String>>() {
                            @Override
                            public Tuple2<Boolean, String> map(final String value) {
                                try {
                                    final String[] parts = value.split(",");
                                    if (parts.length != 3) {
                                        throw new IllegalArgumentException("Invalid format");
                                    }
                                    Integer.parseInt(parts[0]);
                                    if (parts[1].isEmpty() || parts[2].isEmpty()) {
                                        throw new IllegalArgumentException("Empty fields");
                                    }
                                    return Tuple2.of(true, value);
                                } catch (final Exception e) {
                                    return Tuple2.of(
                                            false, value + " [ERROR: " + e.getMessage() + "]");
                                }
                            }
                        });

        // 2. 分流：有效数据 vs 死信
        final DataStream<String> validStream =
                parsed.filter((FilterFunction<Tuple2<Boolean, String>>) t -> t.f0)
                        .map((MapFunction<Tuple2<Boolean, String>, String>) t -> "VALID: " + t.f1);

        final DataStream<String> deadLetterStream =
                parsed.filter((FilterFunction<Tuple2<Boolean, String>>) t -> !t.f0)
                        .map((MapFunction<Tuple2<Boolean, String>, String>) t -> "DLQ: " + t.f1);

        // 3. 输出
        validStream.print("Valid").setParallelism(1);
        deadLetterStream.print("DeadLetter").setParallelism(1);
    }
}
