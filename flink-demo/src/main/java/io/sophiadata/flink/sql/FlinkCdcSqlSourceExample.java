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

/**
 * Flink SQL CDC Source 示例 —— 用 SQL DDL 读取 MySQL CDC 数据。
 *
 * <p>这是最常用的 Flink CDC 入门方式，无需编写 Java 代码。
 *
 * <p>使用方式：
 *
 * <pre>
 * bin/run-demo.sh io.sophiadata.flink.sql.FlinkCdcSqlSourceExample \
 *   --mysql.hostname localhost --mysql.port 3306 \
 *   --mysql.username root --mysql.password root \
 *   --mysql.databaseList mydb --mysql.tableList mydb.users
 * </pre>
 *
 * <p>教学要点：
 *
 * <ul>
 *   <li>mysql-cdc connector 的 DDL 定义
 *   <li>PRIMARY KEY 和 watermark 的配置
 *   <li>CDC 数据的查询和过滤
 * </ul>
 */
public class FlinkCdcSqlSourceExample extends BaseSql {

    public static void main(final String[] args) throws Exception {
        new FlinkCdcSqlSourceExample().init(args, "Flink_CDC_SQL_Source");
    }

    @Override
    public void handle(
            final String[] args,
            final StreamExecutionEnvironment env,
            final StreamTableEnvironment tEnv)
            throws Exception {

        final String hostname = getArg(args, "mysql.hostname", "localhost");
        final String port = getArg(args, "mysql.port", "3306");
        final String username = getArg(args, "mysql.username", "root");
        final String password = getArg(args, "mysql.password", "root");
        final String database = getArg(args, "mysql.databaseList", "mydb");
        final String table = getArg(args, "mysql.tableList", "mydb.users");

        // 1. 定义 MySQL CDC Source
        tEnv.executeSql(
                "CREATE TABLE mysql_cdc (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
                        + "  email STRING,\n"
                        + "  create_time TIMESTAMP(3),\n"
                        + "  PRIMARY KEY (id) NOT ENFORCED,\n"
                        + "  WATERMARK FOR create_time AS create_time - INTERVAL '5' SECOND\n"
                        + ") WITH (\n"
                        + "  'connector' = 'mysql-cdc',\n"
                        + "  'hostname' = '"
                        + hostname
                        + "',\n"
                        + "  'port' = '"
                        + port
                        + "',\n"
                        + "  'username' = '"
                        + username
                        + "',\n"
                        + "  'password' = '"
                        + password
                        + "',\n"
                        + "  'database-list' = '"
                        + database
                        + "',\n"
                        + "  'table-list' = '"
                        + table
                        + "'\n"
                        + ")");

        // 2. 查询 CDC 数据
        final Table result = tEnv.sqlQuery("SELECT * FROM mysql_cdc");
        result.execute().print();

        // 3. 过滤和聚合示例
        // final Table filtered = tEnv.sqlQuery(
        //     "SELECT name, COUNT(*) as cnt FROM mysql_cdc GROUP BY name");
        // filtered.execute().print();
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
