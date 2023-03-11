/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sophiadata.flink.cdc;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.sophiadata.flink.base.BaseSql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** (@SophiaData) (@date 2022/10/25 10:56). */
public class FlinkSqlTest extends BaseSql {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlTest.class);

    public static void main(String[] args) throws Exception {
        new FlinkSqlTest().init(args, "flink_sql_job_test", true, true);
        LOG.info(" init 方法正常 ");
    }

    @Override
    public void handle(String[] args, StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);
        String hostname = params.get("hostname", "localhost");
        int port = params.getInt("port", 3306);
        String username = params.get("username", "root");
        String password = params.get("password", "123456");
        String databaseName = params.get("databaseName", "test");
        String tableName = params.get("tableName", "test2");

        tEnv.executeSql(
                "CREATE TABLE mysql_binlog (\n"
                        + " id INT NOT NULL,\n"
                        + " student STRING,\n"
                        + " sex STRING,\n"
                        + " PRIMARY KEY(id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + " 'connector' = 'mysql-cdc',\n"
                        + " 'hostname' = '"
                        + hostname
                        + "',\n"
                        + " 'port' = '"
                        + port
                        + "',\n"
                        + " 'username' = '"
                        + username
                        + "',\n"
                        + " 'password' = '"
                        + password
                        + "',\n"
                        + " 'database-name' = '"
                        + databaseName
                        + "',\n"
                        + " 'table-name' = '"
                        + tableName
                        + "', \n"
                        + " 'debezium.decimal.handling.mode' = 'string',"
                        + " 'scan.startup.specific-offset.file' = 'mysql-bin.000009',"
                        + "'scan.startup.specific-offset.pos' = '5767')"
                        + "");

        tEnv.executeSql(
                "CREATE TABLE mysql_binlog2 (\n"
                        + " id INT NOT NULL,\n"
                        + " student STRING,\n"
                        + " sex STRING,\n"
                        + " PRIMARY KEY(id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + " 'connector' = 'mysql-cdc',\n"
                        + " 'hostname' = '"
                        + hostname
                        + "',\n"
                        + " 'port' = '"
                        + port
                        + "',\n"
                        + " 'username' = '"
                        + username
                        + "',\n"
                        + " 'password' = '"
                        + password
                        + "',\n"
                        + " 'database-name' = '"
                        + databaseName
                        + "',\n"
                        + " 'table-name' = '"
                        + tableName
                        + "', \n"
                        + " 'debezium.decimal.handling.mode' = 'string'"
                        + " )"
                        + "");

        Table sqlQuery = tEnv.sqlQuery("select id, student, sex from mysql_binlog2");
        tEnv.toChangelogStream(sqlQuery).print();
        try {
            env.execute();
        } catch (Exception e) {
            LOG.error(FlinkSqlTest.class.getSimpleName() + " 异常信息输出：", e);
        }
    }
}
