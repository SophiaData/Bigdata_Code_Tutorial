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

package io.sophiadata.flink.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.sophiadata.flink.base.BaseCode;

/**
 * JDBC Sink 示例 —— 用 JdbcSink 写入 MySQL/PostgreSQL。
 *
 * <p>演示 JdbcSink 的 4 个参数：SQL、StatementBuilder、ExecutionOptions、ConnectionOptions。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.sink.JdbcSinkExample \
 *   --jdbc.url jdbc:mysql://localhost:3306/test \
 *   --jdbc.user root --jdbc.password root
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>JdbcSink.sink() 的参数配置
 *   <li>批量写入配置（batchSize / batchIntervalMs）
 *   <li>重试策略（maxRetries）
 *   <li>Upsert 模式（ON DUPLICATE KEY UPDATE）
 * </ul>
 */
public class JdbcSinkExample extends BaseCode {

    public static void main(final String[] args) throws Exception {
        new JdbcSinkExample().init(args, "JDBC_Sink_Example");
    }

    @Override
    public void handle(final String[] args, final StreamExecutionEnvironment env) throws Exception {
        final String jdbcUrl = getArg(args, "jdbc.url", "jdbc:mysql://localhost:3306/test");
        final String jdbcUser = getArg(args, "jdbc.user", "root");
        final String jdbcPassword = getArg(args, "jdbc.password", "root");

        // 1. 模拟数据流
        // 实际使用时取消下面注释，配置正确的 JDBC 参数即可写入数据库
        //
        // env.fromElements(Tuple2.of(1, "Alice"), Tuple2.of(2, "Bob"), Tuple2.of(3, "Charlie"))
        //     .addSink(JdbcSink.sink(
        //         "INSERT INTO users (id, name) VALUES (?, ?)
        //          ON DUPLICATE KEY UPDATE name = VALUES(name)",
        //         (ps, t) -> {
        //             ps.setInt(1, t.f0);
        //             ps.setString(2, t.f1);
        //         },
        //         JdbcExecutionOptions.builder()
        //             .withBatchSize(100)
        //             .withBatchIntervalMs(200)
        //             .withMaxRetries(3)
        //             .build(),
        //         new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        //             .withUrl(jdbcUrl)
        //             .withDriverName("com.mysql.cj.jdbc.Driver")
        //             .withUsername(jdbcUser)
        //             .withPassword(jdbcPassword)
        //             .build()))
        //     .name("JDBC_Sink");

        // 2. 仅打印演示（无需真实数据库）
        env.fromElements(Tuple2.of(1, "Alice"), Tuple2.of(2, "Bob"), Tuple2.of(3, "Charlie"))
                .map(t -> "Would insert: id=" + t.f0 + ", name=" + t.f1)
                .print("JDBC_Sink_Demo")
                .setParallelism(1);
    }

    private static String getArg(final String[] args, final String key, final String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (("--" + key).equals(args[i])) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
}
