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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import io.sophiadata.flink.base.BaseCode;

/**
 * CoProcessFunction 示例 —— 双流处理（配置热更新场景）。
 *
 * <p>场景：数据流 + 配置流，配置流动态更新处理逻辑。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.streaming.advanced.CoProcessFunctionExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>CoProcessFunction 的 processElement1 / processElement2
 *   <li>双流状态管理
 *   <li>配置热更新模式
 * </ul>
 */
public class CoProcessFunctionExample extends BaseCode {

    public static void main(final String[] args) throws Exception {
        new CoProcessFunctionExample().init(args, "CoProcess_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        // 1. 数据流：(userId, value)
        final DataStream<Tuple2<String, Double>> dataStream =
                env.fromElements(
                        Tuple2.of("user_1", 100.0),
                        Tuple2.of("user_2", 200.0),
                        Tuple2.of("user_1", 150.0));

        // 2. 配置流：(userId, threshold)
        final DataStream<Tuple2<String, Double>> configStream =
                env.fromElements(Tuple2.of("user_1", 120.0), Tuple2.of("user_2", 250.0));

        // 3. CoProcess：超过阈值则告警
        final SingleOutputStreamOperator<String> result =
                dataStream
                        .connect(configStream)
                        .keyBy(t -> t.f0, t -> t.f0)
                        .process(new AlertCoProcessFunction());

        result.print("Alert").setParallelism(1);
    }

    /** 双流处理：数据流 + 配置流，超过阈值触发告警。 */
    public static class AlertCoProcessFunction
            extends CoProcessFunction<Tuple2<String, Double>, Tuple2<String, Double>, String> {

        private static final long serialVersionUID = 1L;
        private transient ValueState<Double> thresholdState;

        @Override
        public void open(final Configuration parameters) throws Exception {
            thresholdState =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("threshold", Double.class));
        }

        @Override
        public void processElement1(
                final Tuple2<String, Double> value, final Context ctx, final Collector<String> out)
                throws Exception {
            final Double threshold = thresholdState.value();
            if (threshold != null && value.f1 > threshold) {
                out.collect(
                        "ALERT: "
                                + value.f0
                                + " value "
                                + value.f1
                                + " exceeds threshold "
                                + threshold);
            }
        }

        @Override
        public void processElement2(
                final Tuple2<String, Double> config, final Context ctx, final Collector<String> out)
                throws Exception {
            // 更新配置
            thresholdState.update(config.f1);
            out.collect("Config updated: " + config.f0 + " threshold=" + config.f1);
        }
    }
}
