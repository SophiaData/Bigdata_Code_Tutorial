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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.sophiadata.flink.base.BaseSql;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;

/**
 * Flink SQL 维表关联示例 —— 演示 Lookup Join 关联维表数据。
 *
 * <p>场景：订单流关联商品维表， enrich 订单的商品名称和价格。使用 fromValues 构造内存数据源， 无需外部数据库即可运行。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.sql.FlinkSqlLookupJoinExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>Lookup Join 语法：主表 JOIN 维表 FOR SYSTEM_TIME AS OF 主表时间
 *   <li>维表定义：用 SQL DDL 创建维表（fromValues 模拟）
 *   <li>Temporal Join 与 Lookup Join 的区别
 *   <li>生产环境：维表通常来自 JDBC/Redis/HBase
 * </ul>
 */
public class FlinkSqlLookupJoinExample extends BaseSql {

    public static void main(final String[] args) throws Exception {
        new FlinkSqlLookupJoinExample().init(args, "FlinkSQL_LookupJoin_Example");
    }

    @Override
    public void handle(
            final String[] args,
            final StreamExecutionEnvironment env,
            final StreamTableEnvironment tableEnv)
            throws Exception {
        // 用 fromValues 构造订单数据
        final Table orderData =
                tableEnv.fromValues(
                        "(STRING, STRING, INT, TIMESTAMP(3))",
                        Arrays.asList(
                                org.apache.flink.types.Row.of(
                                        "order_1",
                                        "p_1",
                                        2,
                                        Timestamp.valueOf("2024-01-01 10:00:00")),
                                org.apache.flink.types.Row.of(
                                        "order_2",
                                        "p_2",
                                        1,
                                        Timestamp.valueOf("2024-01-01 10:01:00")),
                                org.apache.flink.types.Row.of(
                                        "order_3",
                                        "p_1",
                                        3,
                                        Timestamp.valueOf("2024-01-01 10:02:00"))));

        // 用 fromValues 构造商品维表
        final Table productData =
                tableEnv.fromValues(
                        "(STRING, STRING, DECIMAL(10,2))",
                        Arrays.asList(
                                org.apache.flink.types.Row.of(
                                        "p_1", "iPhone 15", new BigDecimal("999.00")),
                                org.apache.flink.types.Row.of(
                                        "p_2", "MacBook Pro", new BigDecimal("2999.00")),
                                org.apache.flink.types.Row.of(
                                        "p_3", "AirPods", new BigDecimal("299.00"))));

        tableEnv.createTemporaryView("order_stream", orderData);
        tableEnv.createTemporaryView("product_dim", productData);

        // Lookup Join：订单关联商品维表
        final Table result =
                tableEnv.sqlQuery(
                        "SELECT "
                                + "  o.f0 AS order_id,"
                                + "  o.f1 AS product_id,"
                                + "  o.f2 AS amount,"
                                + "  p.f1 AS product_name,"
                                + "  p.f2 AS price,"
                                + "  o.f2 * p.f2 AS total_price "
                                + "FROM order_stream AS o "
                                + "JOIN product_dim FOR SYSTEM_TIME AS OF o.order_time AS p "
                                + "ON o.f1 = p.f0");

        tableEnv.toDataStream(result).print("LookupJoin_Result").setParallelism(1);
    }
}
