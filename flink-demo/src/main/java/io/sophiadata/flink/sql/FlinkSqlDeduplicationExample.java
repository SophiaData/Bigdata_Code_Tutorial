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

import java.sql.Timestamp;
import java.util.Arrays;

/**
 * Flink SQL 去重示例 —— 演示 ROW_NUMBER 去重和 Deduplication。
 *
 * <p>场景：用户行为流中存在重复事件，按 userId 去重保留最新一条。使用 fromValues 构造内存数据源， 无需外部依赖即可运行。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.sql.FlinkSqlDeduplicationExample
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>ROW_NUMBER() + PARTITION BY + ORDER BY 实现去重
 *   <li>子查询过滤 row_num = 1 保留最新记录
 *   <li>Flink SQL 流式去重的原理（状态 + 过期）
 *   <li>生产环境：通常配合 proctime/watermark 使用
 * </ul>
 */
public class FlinkSqlDeduplicationExample extends BaseSql {

    public static void main(final String[] args) throws Exception {
        new FlinkSqlDeduplicationExample().init(args, "FlinkSQL_Deduplication_Example");
    }

    @Override
    public void handle(
            final String[] args,
            final StreamExecutionEnvironment env,
            final StreamTableEnvironment tableEnv)
            throws Exception {
        // 用 fromValues 构造带重复的数据
        final Table rawData =
                tableEnv.fromValues(
                        "(STRING, STRING, BIGINT, TIMESTAMP(3))",
                        Arrays.asList(
                                org.apache.flink.types.Row.of(
                                        "user_1",
                                        "login",
                                        1L,
                                        Timestamp.valueOf("2024-01-01 10:00:00")),
                                org.apache.flink.types.Row.of(
                                        "user_1",
                                        "click",
                                        2L,
                                        Timestamp.valueOf("2024-01-01 10:00:01")),
                                org.apache.flink.types.Row.of(
                                        "user_1",
                                        "login",
                                        3L,
                                        Timestamp.valueOf("2024-01-01 10:00:02")),
                                org.apache.flink.types.Row.of(
                                        "user_2",
                                        "click",
                                        4L,
                                        Timestamp.valueOf("2024-01-01 10:00:03")),
                                org.apache.flink.types.Row.of(
                                        "user_2",
                                        "login",
                                        5L,
                                        Timestamp.valueOf("2024-01-01 10:00:04")),
                                org.apache.flink.types.Row.of(
                                        "user_1",
                                        "logout",
                                        6L,
                                        Timestamp.valueOf("2024-01-01 10:00:05"))));

        tableEnv.createTemporaryView("user_events", rawData);

        // ROW_NUMBER 去重：按 userId 分组，按 event_time 降序排列，取 row_num = 1 的记录
        final Table deduplicated =
                tableEnv.sqlQuery(
                        "SELECT f0 AS user_id, f1 AS event_type, f2 AS event_id, f3 AS event_time "
                                + "FROM ("
                                + "  SELECT *, "
                                + "    ROW_NUMBER() OVER (PARTITION BY f0 ORDER BY f3 DESC) AS row_num "
                                + "  FROM user_events"
                                + ") t "
                                + "WHERE row_num = 1");

        tableEnv.toDataStream(deduplicated).print("Dedup_Result").setParallelism(1);
    }
}
