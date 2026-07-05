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

package io.sophiadata.flink.sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import io.sophiadata.flink.base.BaseCode;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Flink CEP 示例 —— 模式识别（连续登录失败检测）。
 *
 * <p>场景：检测用户连续 3 次登录失败。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.sql.FlinkCepExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>Pattern 定义：条件、次数、时间约束
 *   <li>CEP.pattern() 创建 PatternStream
 *   <li>PatternSelectFunction 处理匹配结果
 * </ul>
 */
public class FlinkCepExample extends BaseCode {

    public static void main(final String[] args) throws Exception {
        new FlinkCepExample().init(args, "Flink_CEP_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        // 模拟登录事件流：(userId, action, timestamp)
        final DataStream<Tuple3<String, String, Long>> loginStream =
                env.fromElements(
                                Tuple3.of("user_1", "login_fail", 1000L),
                                Tuple3.of("user_1", "login_fail", 2000L),
                                Tuple3.of("user_1", "login_fail", 3000L),
                                Tuple3.of("user_2", "login_success", 4000L),
                                Tuple3.of("user_3", "login_fail", 5000L),
                                Tuple3.of("user_3", "login_fail", 6000L))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(
                                                Duration.ZERO)
                                        .withTimestampAssigner((event, ts) -> event.f2));

        // 1. 定义 CEP 模式：连续 3 次登录失败
        final Pattern<Tuple3<String, String, Long>, ?> pattern =
                Pattern.<Tuple3<String, String, Long>>begin("start")
                        .where(
                                new SimpleCondition<Tuple3<String, String, Long>>() {
                                    @Override
                                    public boolean filter(
                                            final Tuple3<String, String, Long> value) {
                                        return "login_fail".equals(value.f1);
                                    }
                                })
                        .times(3)
                        .within(Time.seconds(10));

        // 2. 应用 CEP 模式
        final PatternStream<Tuple3<String, String, Long>> patternStream =
                CEP.pattern(loginStream.keyBy(t -> t.f0), pattern);

        // 3. 处理匹配结果
        final DataStream<String> alerts =
                patternStream.select(
                        new PatternSelectFunction<Tuple3<String, String, Long>, String>() {
                            @Override
                            public String select(
                                    final Map<String, List<Tuple3<String, String, Long>>> pattern) {
                                final List<Tuple3<String, String, Long>> start =
                                        pattern.get("start");
                                return "ALERT: "
                                        + start.get(0).f0
                                        + " failed login 3 times consecutively!";
                            }
                        });

        alerts.print("CEP_Alert").setParallelism(1);
    }
}
