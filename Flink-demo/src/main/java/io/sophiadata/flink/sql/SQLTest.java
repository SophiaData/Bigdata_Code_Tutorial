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

package io.sophiadata.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.sophiadata.flink.base.BaseSql;

/** (@gtk) (@date 2022/12/22 18:35). */
public class SQLTest extends BaseSql {
    public static void main(String[] args) throws Exception {
        new SQLTest().init(args, "SQLTest");
    }

    @Override
    public void handle(String[] args, StreamExecutionEnvironment env, StreamTableEnvironment tEnv)
            throws Exception {
        tEnv.executeSql(
                "CREATE TABLE test2 (\n"
                        + "  id STRING,\n"
                        + "  op STRING,\n"
                        + "  sex STRING,\n"
                        + "  PRIMARY KEY (id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "   'connector' = 'jdbc',\n"
                        + "   'url' = 'jdbc:mysql://localhost:3306/test',\n"
                        + "   'table-name' = 'test2',\n"
                        + " 'username' = 'root',"
                        + "'password' = '123456'"
                        + ");");

        Table table = tEnv.sqlQuery("select * from test2");

        table.execute().print();
    }
}
