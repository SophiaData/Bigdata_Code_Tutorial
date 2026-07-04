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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink SQL Window 示例 —— 演示 Tumble / Hop / Session 三种窗口的用法。
 *
 * <p>使用 fromValues 构造内存数据源，无需外部依赖即可运行。
 *
 * <p>三种窗口类型：
 *
 * <ul>
 *   <li><b>Tumble</b>：固定大小、不重叠的滚动窗口
 *   <li><b>Hop</b>：固定大小、可重叠的滑动窗口
 *   <li><b>Session</b>：根据活跃度动态划分的会话窗口
 * </ul>
 */
public class WindowSQLExample {

    public static void main(final String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 创建带事件时间的订单数据源
        tEnv.executeSql(
                "CREATE TABLE orders (\n"
                        + "  order_id STRING,\n"
                        + "  amount DOUBLE,\n"
                        + "  event_time TIMESTAMP(3),\n"
                        + "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '10',\n"
                        + "  'fields.order_id.length' = '8',\n"
                        + "  'fields.amount.min' = '10.0',\n"
                        + "  'fields.amount.max' = '1000.0'\n"
                        + ")");

        // Tumble 窗口：每 10 秒统计一次订单总额
        System.out.println("=== Tumble Window: 每 10 秒统计订单总额 ===");
        tEnv.executeSql(
                        "SELECT \n"
                                + "  TUMBLE_START(event_time, INTERVAL '10' SECOND) AS window_start,\n"
                                + "  TUMBLE_END(event_time, INTERVAL '10' SECOND) AS window_end,\n"
                                + "  COUNT(*) AS order_count,\n"
                                + "  SUM(amount) AS total_amount\n"
                                + "FROM orders\n"
                                + "GROUP BY TUMBLE(event_time, INTERVAL '10' SECOND)")
                .print();

        // Hop 窗口：每 5 秒滑动，窗口大小 10 秒
        System.out.println("\n=== Hop Window: 每 5 秒滑动，窗口大小 10 秒 ===");
        tEnv.executeSql(
                        "SELECT \n"
                                + "  HOP_START(event_time, INTERVAL '5' SECOND, INTERVAL '10' SECOND) AS window_start,\n"
                                + "  HOP_END(event_time, INTERVAL '5' SECOND, INTERVAL '10' SECOND) AS window_end,\n"
                                + "  COUNT(*) AS order_count,\n"
                                + "  SUM(amount) AS total_amount\n"
                                + "FROM orders\n"
                                + "GROUP BY HOP(event_time, INTERVAL '5' SECOND, INTERVAL '10' SECOND)")
                .print();

        // Session 窗口：活跃度间隔 5 秒的会话窗口
        System.out.println("\n=== Session Window: 活跃度间隔 5 秒 ===");
        tEnv.executeSql(
                        "SELECT \n"
                                + "  SESSION_START(event_time, INTERVAL '5' SECOND) AS session_start,\n"
                                + "  SESSION_END(event_time, INTERVAL '5' SECOND) AS session_end,\n"
                                + "  COUNT(*) AS order_count,\n"
                                + "  SUM(amount) AS total_amount\n"
                                + "FROM orders\n"
                                + "GROUP BY SESSION(event_time, INTERVAL '5' SECOND)")
                .print();
    }
}
