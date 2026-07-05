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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** 集成测试：验证 Flink SQL CDC DDL 语法。 */
public class FlinkCdcSqlSourceExampleIT {

    @Test
    public void testMysqlCdcDdlCanBeCreated() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE test_cdc (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
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

        final Table table = tEnv.from("test_cdc");
        assertThat(table).isNotNull();
    }
}
