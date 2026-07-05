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

import io.sophiadata.flink.base.BaseSql;

/**
 * MySQL CDC → Kafka Sink（Flink SQL 版本）。
 *
 * <p>演示用 Flink SQL DDL 同时定义 MySQL CDC Source 和 Kafka Sink，将 CDC 事件写入 Kafka Topic。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.sink.MysqlCdcToKafkaSqlPipeline \
 *   --mysql.hostname localhost --mysql.port 3306 \
 *   --mysql.username root --mysql.password root \
 *   --mysql.databaseList mydb --mysql.tableList mydb.users \
 *   --kafka.brokers localhost:9092 --kafka.topic cdc-events
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>Flink SQL DDL 定义 MySQL CDC Source
 *   <li>Kafka Sink 的 connector / topic / format 配置
 *   <li>如何在 SQL 中处理 CDC 操作类型
 * </ul>
 */
public class MysqlCdcToKafkaSqlPipeline extends BaseSql {

    public static void main(final String[] args) throws Exception {
        new MysqlCdcToKafkaSqlPipeline().init(args, "MySQL_CDC_to_Kafka_SQL");
    }

    @Override
    public void handle(
            final String[] args,
            final StreamExecutionEnvironment env,
            final StreamTableEnvironment tEnv)
            throws Exception {

        // 默认参数值（可通过 --key value 覆盖）
        final String mysqlHostname = getArg(args, "mysql.hostname", "localhost");
        final String mysqlPort = getArg(args, "mysql.port", "3306");
        final String mysqlUsername = getArg(args, "mysql.username", "root");
        final String mysqlPassword = getArg(args, "mysql.password", "root");
        final String mysqlDatabase = getArg(args, "mysql.databaseList", "mydb");
        final String mysqlTable = getArg(args, "mysql.tableList", "mydb.users");
        final String kafkaBrokers = getArg(args, "kafka.brokers", "localhost:9092");
        final String kafkaTopic = getArg(args, "kafka.topic", "cdc-events");

        // 1. 定义 MySQL CDC Source（读取 binlog）
        tEnv.executeSql(
                "CREATE TABLE mysql_cdc (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
                        + "  email STRING,\n"
                        + "  PRIMARY KEY (id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'mysql-cdc',\n"
                        + "  'hostname' = '"
                        + mysqlHostname
                        + "',\n"
                        + "  'port' = '"
                        + mysqlPort
                        + "',\n"
                        + "  'username' = '"
                        + mysqlUsername
                        + "',\n"
                        + "  'password' = '"
                        + mysqlPassword
                        + "',\n"
                        + "  'database-list' = '"
                        + mysqlDatabase
                        + "',\n"
                        + "  'table-list' = '"
                        + mysqlTable
                        + "'\n"
                        + ")");

        // 2. 定义 Kafka Sink（写入 Topic）
        tEnv.executeSql(
                "CREATE TABLE kafka_sink (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
                        + "  email STRING,\n"
                        + "  op STRING,\n"
                        + "  PRIMARY KEY (id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = '"
                        + kafkaTopic
                        + "',\n"
                        + "  'properties.bootstrap.servers' = '"
                        + kafkaBrokers
                        + "',\n"
                        + "  'format' = 'json',\n"
                        + "  'scan.startup.mode' = 'latest-offset'\n"
                        + ")");

        // 3. 将 CDC 数据写入 Kafka
        // 注意：CDC Source 不直接提供 op 字段，这里用 INSERT 语句演示基本的 CDC → Kafka 流
        tEnv.executeSql(
                "INSERT INTO kafka_sink\n"
                        + "SELECT id, name, email, 'c' AS op\n"
                        + "FROM mysql_cdc");
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
