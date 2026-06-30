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

package io.sophiadata.flink.paimon;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * MySQL to Paimon whole-database sync example using Flink CDC Pipeline API.
 *
 * <p>This example demonstrates how to sync all tables from a MySQL database to Apache Paimon data
 * lake.
 *
 * <p>Usage:
 *
 * <pre>
 *   flink run -c io.sophiadata.flink.paimon.MySqlToPaimonPipeline \
 *     flink-paimon-demo-1.0.0.jar \
 *     --mysql.host localhost \
 *     --mysql.port 3306 \
 *     --mysql.database source_db \
 *     --mysql.username root \
 *     --mysql.password root \
 *     --paimon.path /path/to/paimon/catalog
 * </pre>
 */
@SuppressWarnings("PMD.UseUtilityClass")
public class MySqlToPaimonPipeline {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String mysqlHost = params.get("mysql.host", "localhost");
        int mysqlPort = params.getInt("mysql.port", 3306);
        String mysqlDatabase = params.get("mysql.database", "source_db");
        String mysqlUsername = params.get("mysql.username", "root");
        String mysqlPassword = params.get("mysql.password", "root");
        String paimonPath = params.get("paimon.path", "file:///tmp/paimon/catalog");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. Create MySQL CDC catalog
        tableEnv.executeSql(
                "CREATE CATALOG mysql_catalog WITH ("
                        + "  'type' = 'mysql-cdc',"
                        + "  'hostname' = '"
                        + mysqlHost
                        + "',"
                        + "  'port' = '"
                        + mysqlPort
                        + "',"
                        + "  'username' = '"
                        + mysqlUsername
                        + "',"
                        + "  'password' = '"
                        + mysqlPassword
                        + "',"
                        + "  'database-name' = '"
                        + mysqlDatabase
                        + "'"
                        + ")");

        // 2. Create Paimon catalog
        tableEnv.executeSql(
                "CREATE CATALOG paimon_catalog WITH ("
                        + "  'type' = 'paimon',"
                        + "  'warehouse' = '"
                        + paimonPath
                        + "'"
                        + ")");

        // 3. Create target database in Paimon
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS paimon_catalog." + mysqlDatabase);

        // 4. Sync each table
        String[] tables = {"users", "orders", "products"};
        for (String table : tables) {
            // Create target table schema in Paimon
            tableEnv.executeSql(
                    "CREATE TABLE IF NOT EXISTS paimon_catalog."
                            + mysqlDatabase
                            + "."
                            + table
                            + " ("
                            + "  id BIGINT,"
                            + "  name STRING,"
                            + "  age INT,"
                            + "  create_time TIMESTAMP(3),"
                            + "  PRIMARY KEY (id) NOT ENFORCED"
                            + ") WITH ("
                            + "  'connector' = 'paimon',"
                            + "  'path' = '"
                            + paimonPath
                            + "/"
                            + mysqlDatabase
                            + "/"
                            + table
                            + "'"
                            + ")");

            // Sync data from MySQL to Paimon
            tableEnv.executeSql(
                    "INSERT INTO paimon_catalog."
                            + mysqlDatabase
                            + "."
                            + table
                            + " SELECT * FROM mysql_catalog."
                            + mysqlDatabase
                            + "."
                            + table);
        }

        env.execute("MySQL to Paimon Sync - " + mysqlDatabase);
    }
}
