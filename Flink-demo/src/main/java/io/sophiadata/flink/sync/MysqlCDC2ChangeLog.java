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

package io.sophiadata.flink.sync;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;

/** (@SophiaData) (@date 2023/3/7 20:29). */
public class MysqlCDC2ChangeLog {
    // refer : https://github.com/GourdErwa/flink-advanced.git
    public static void main(String[] args) throws Exception {
        final Configuration flinkConfiguration = new Configuration();
        flinkConfiguration.setString("rest.bind-port", "8081-8089");
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfiguration);
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 开启checkpoint
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);

        MySqlSource<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> mySqlSource =
                MySqlSource.<RowKindJsonDeserializationSchemaBase.TableIRowKindJson>builder()
                        .hostname("10.0.59.xx")
                        .port(3306)
                        .scanNewlyAddedTableEnabled(true)
                        .databaseList("example_cdc")
                        .tableList("example_cdc.common_user,example_cdc.order")
                        .startupOptions(StartupOptions.initial())
                        .includeSchemaChanges(true)
                        .username("root")
                        .password("123456")
                        // 更多扩展支持分流序列化器参考 io.group.flink.stream.cdc.schema 包内容
                        .deserializer(new RowKindJsonDeserializationSchemaV2())
                        .build();

        final DataStreamSource<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> source =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        // sink table
        new ChangelogSink().compute(source, tableEnv);
        // sink iceberg table
        // new IcebergCdcSink().compute(source, tableEnv);

        env.execute("MySQL CDC + Changelog");
    }
}
