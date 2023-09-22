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

package io.sophiadata.flink.udf;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** (@sophiadata) (@date 2023/9/21 15:57). */
public class SQLTest1 {

    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // set sql job name
        tEnv.getConfig().getConfiguration().setString("pipeline.name", "SQLTest");

        DataStreamSource<Tuple4<Integer, String, String, String>> tuple4DataStreamSource =
                env.fromElements(
                        Tuple4.of(
                                200,
                                "e40153bd-8a88-46e1-7996-47e1cd77fe1c",
                                "136",
                                "8z!8k1!80"));

        // 将输入数据流注册为表
        tEnv.createTemporaryView("myTable", tuple4DataStreamSource);

        //        Table table = tEnv.sqlQuery("select * from myTable");
        //
        //        table.execute().print();

        tEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

        Table table1 =
                tEnv.sqlQuery(
                        "SELECT * "
                                + " FROM myTable, LATERAL TABLE(SplitFunction(f3,'!')) as T(temp)");

        table1.execute().print();
    }
}
