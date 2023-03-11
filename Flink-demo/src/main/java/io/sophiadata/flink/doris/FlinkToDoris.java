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

package io.sophiadata.flink.doris;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import io.sophiadata.flink.base.BaseCode;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/** (@SophiaData) (@date 2022/10/25 10:56). */
public class FlinkToDoris extends BaseCode {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkToDoris.class);

    public static void main(String[] args) throws Exception {
        // 通过 light schema change 实现 flink ddl 自动同步 Doris
        new FlinkToDoris().init(args, "flink_doris_test", true, true);
        LOG.info(" init 方法正常 ");
    }

    @Override
    public void handle(String[] args, StreamExecutionEnvironment env) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);
        String hostname = params.get("hostname", "localhost");
        int port = params.getInt("port", 3306);
        String username = params.get("username", "root");
        String password = params.get("password", "123456");
        String databaseList = params.get("databaseList", "test");
        String tableList = params.get("tableList", "test.test2");

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        JsonDebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(false, customConverterConfigs);

        MySqlSource<String> sourceFunction =
                MySqlSource.<String>builder()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .databaseList(databaseList)
                        .tableList(tableList)
                        .deserializer(schema)
                        .includeSchemaChanges(true) // 接收 ddl
                        .startupOptions(StartupOptions.initial())
                        .serverTimeZone("Asia/Shanghai")
                        .debeziumProperties(DateToStringConverter.defaultProp)
                        .build();

        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        DorisOptions dorisOptions =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("test.t1")
                        .setUsername("root")
                        .setPassword("")
                        .build();
        //
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
                .setLabelPrefix("label-doris" + UUID.randomUUID())
                .setStreamLoadProp(props)
                .setDeletable(true);

        DorisSink.Builder<String> builder = DorisSink.builder();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setDorisOptions(dorisOptions)
                .setSerializer(
                        JsonDebeziumSchemaSerializer.builder()
                                .setDorisOptions(dorisOptions)
                                .build());

        env.fromSource(
                        sourceFunction,
                        WatermarkStrategy.noWatermarks(),
                        "MySQL Source") // .print();
                .sinkTo(builder.build());
    }
}
