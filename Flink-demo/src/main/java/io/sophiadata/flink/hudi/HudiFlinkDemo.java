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

package io.sophiadata.flink.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.sophiadata.flink.base.BaseSql;

/** (@SophiaData) (@date 2022/10/30 16:28). */
public class HudiFlinkDemo extends BaseSql {

    public static void main(String[] args) throws Exception {
        //
        new HudiFlinkDemo().init(args, "HudiFlinkDemo", false, true, "hdfs://");
    }

    @Override
    public void handle(String[] args, StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.executeSql(
                "CREATE TABLE sourceT (\n"
                        + "  uuid varchar(20),\n"
                        + "  name varchar(10),\n"
                        + "  age int,\n"
                        + "  ts timestamp(3),\n"
                        + "  `partition` varchar(20)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '1'\n"
                        + ")");

        tEnv.executeSql(
                "create table t2(\n"
                        + "  uuid varchar(20),\n"
                        + "  name varchar(10),\n"
                        + "  age int,\n"
                        + "  ts timestamp(3),\n"
                        + "  `partition` varchar(20)\n"
                        + ")\n"
                        + "with (\n"
                        + "  'connector' = 'hudi',\n"
                        + "  'path' = '/tmp/hudi_flink/t2',\n"
                        + "  'table.type' = 'MERGE_ON_READ'\n"
                        + ")");

        tEnv.executeSql("insert into t2 select * from sourceT");
    }
}
