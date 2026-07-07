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

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.sophiadata.flink.base.BaseCode;

/**
 * 自定义 Partitioner 示例 —— 演示自定义数据分区策略。
 *
 * <p>场景：按数据的区域字段（region）将数据分发到不同的下游算子，实现区域隔离处理。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.streaming.advanced.CustomPartitionerExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>Partitioner<T> 接口实现自定义分区逻辑
 *   <li>stream.partitionCustom() 应用自定义分区器
 *   <li>分区数 = 下游算子并行度
 *   <li>适用场景：数据倾斜优化、区域隔离、保证相同 key 分配到同一分区
 * </ul>
 */
public class CustomPartitionerExample extends BaseCode {

    public static void main(final String[] args) throws Exception {
        new CustomPartitionerExample().init(args, "CustomPartitioner_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        final DataStream<Tuple3WithRegion> source =
                env.fromElements(
                        new Tuple3WithRegion("order_1", "user_A", "north"),
                        new Tuple3WithRegion("order_2", "user_B", "south"),
                        new Tuple3WithRegion("order_3", "user_C", "north"),
                        new Tuple3WithRegion("order_4", "user_D", "east"),
                        new Tuple3WithRegion("order_5", "user_E", "south"),
                        new Tuple3WithRegion("order_6", "user_F", "west"),
                        new Tuple3WithRegion("order_7", "user_G", "north"));

        // 自定义分区：按 region 分区
        source.partitionCustom(new RegionPartitioner(), element -> element.region)
                .map(value -> value.orderId + " (" + value.region + ")")
                .print("Partitioner_Result")
                .setParallelism(4);
    }

    /** 包含区域信息的三元组。 */
    public static class Tuple3WithRegion {
        public String orderId;
        public String userId;
        public String region;

        public Tuple3WithRegion() {}

        public Tuple3WithRegion(final String orderId, final String userId, final String region) {
            this.orderId = orderId;
            this.userId = userId;
            this.region = region;
        }
    }

    /** 按区域分区的自定义 Partitioner。 */
    public static class RegionPartitioner implements Partitioner<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public int partition(final String key, final int numPartitions) {
            switch (key) {
                case "north":
                    return 0;
                case "south":
                    return 1;
                case "east":
                    return 2;
                case "west":
                    return 3;
                default:
                    return Math.abs(key.hashCode() % numPartitions);
            }
        }
    }
}
