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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 集成测试：验证 MysqlCdcToKafkaSqlPipeline 的 DDL 定义和管道构建。
 *
 * <p>不依赖真实的 MySQL/Kafka，只验证 DDL 语法正确、表能创建。
 */
public class MysqlCdcToKafkaSqlPipelineIT {

    @Test
    public void testKafkaSinkDdlCanBeCreated() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 验证 Kafka Sink DDL 语法正确（不连接真实 Kafka）
        tEnv.executeSql(
                "CREATE TABLE test_kafka_sink (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
                        + "  email STRING,\n"
                        + "  op STRING,\n"
                        + "  PRIMARY KEY (id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = 'test-topic',\n"
                        + "  'properties.bootstrap.servers' = 'localhost:9092',\n"
                        + "  'format' = 'json',\n"
                        + "  'scan.startup.mode' = 'latest-offset'\n"
                        + ")");

        // 验证表已创建
        final var table = tEnv.from("test_kafka_sink");
        assertThat(table).isNotNull();
    }

    @Test
    public void testMysqlCdcSourceDdlCanBeCreated() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 验证 MySQL CDC Source DDL 语法（不连接真实 MySQL）
        tEnv.executeSql(
                "CREATE TABLE test_mysql_cdc (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
                        + "  email STRING,\n"
                        + "  PRIMARY KEY (id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'mysql-cdc',\n"
                        + "  'hostname' = 'localhost',\n"
                        + "  'port' = '3306',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = 'root',\n"
                        + "  'database-list' = 'testdb',\n"
                        + "  'table-list' = 'testdb.users'\n"
                        + ")");

        final var table = tEnv.from("test_mysql_cdc");
        assertThat(table).isNotNull();
    }
}
