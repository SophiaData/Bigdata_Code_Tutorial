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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * MySQL to Paimon sync using pure SQL (Flink SQL Client compatible).
 *
 * <p>This example shows the SQL-only approach, which can be executed directly in Flink SQL Client
 * without writing Java code.
 *
 * <p>Example SQL statements:
 *
 * <pre>
 * -- 1. Create MySQL catalog
 * CREATE CATALOG mysql_catalog WITH (
 *   'type' = 'mysql-cdc',
 *   'hostname' = 'localhost',
 *   'port' = '3306',
 *   'username' = 'root',
 *   'password' = 'root',
 *   'database-name' = 'source_db'
 * );
 *
 * -- 2. Create Paimon catalog
 * CREATE CATALOG paimon_catalog WITH (
 *   'type' = 'paimon',
 *   'warehouse' = 'file:///tmp/paimon/catalog'
 * );
 *
 * -- 3. Create Paimon table
 * CREATE TABLE paimon_catalog.target_db.users (
 *   id BIGINT,
 *   name STRING,
 *   age INT,
 *   PRIMARY KEY (id) NOT ENFORCED
 * ) WITH (
 *   'connector' = 'paimon',
 *   'path' = 'file:///tmp/paimon/catalog/target_db/users'
 * );
 *
 * -- 4. Sync data
 * INSERT INTO paimon_catalog.target_db.users
 * SELECT * FROM mysql_catalog.source_db.users;
 * </pre>
 */
public class MySqlToPaimonSqlPipeline {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String mysqlHost = params.get("mysql.host", "localhost");
        int mysqlPort = params.getInt("mysql.port", 3306);
        String mysqlDatabase = params.get("mysql.database", "source_db");
        String mysqlUsername = params.get("mysql.username", "root");
        String mysqlPassword = params.get("mysql.password", "root");
        String paimonPath = params.get("paimon.path", "file:///tmp/paimon/catalog");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Create MySQL CDC catalog
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

        // Create Paimon catalog
        tableEnv.executeSql(
                "CREATE CATALOG paimon_catalog WITH ("
                        + "  'type' = 'paimon',"
                        + "  'warehouse' = '"
                        + paimonPath
                        + "'"
                        + ")");

        // Example: Sync users table
        // Create target table in Paimon
        tableEnv.executeSql(
                "CREATE TABLE paimon_catalog."
                        + mysqlDatabase
                        + ".users ("
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
                        + "/users'"
                        + ")");

        // Sync data from MySQL to Paimon
        tableEnv.executeSql(
                "INSERT INTO paimon_catalog."
                        + mysqlDatabase
                        + ".users"
                        + " SELECT * FROM mysql_catalog."
                        + mysqlDatabase
                        + ".users");

        env.execute("MySQL to Paimon SQL Sync");
    }
}
