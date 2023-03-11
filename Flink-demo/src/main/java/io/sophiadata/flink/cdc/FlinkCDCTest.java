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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import io.sophiadata.flink.base.BaseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** (@SophiaData) (@date 2022/5/7 14:17). */
public class FlinkCDCTest extends BaseCode {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkCDCTest.class);

    public static void main(String[] args) throws Exception {
        // 参数信息通过 args 传递
        new FlinkCDCTest().init(args, "flink_cdc_job_test", true, true);
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
        String tableList = params.get("tableList", "test.test3");

        Properties properties = new Properties();
        // decimal 设置为 string 避免转换异常
        properties.put("decimal.handling.mode", "string");

        MySqlSource<String> sourceFunction =
                MySqlSource.<String>builder()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .databaseList(databaseList)
                        .tableList(tableList)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true)
                        .startupOptions(StartupOptions.initial())
                        .debeziumProperties(properties)
                        .build();

        DataStreamSource<String> mysql =
                env.fromSource(sourceFunction, WatermarkStrategy.noWatermarks(), "mysql")
                        .setParallelism(1);

        mysql.print().setParallelism(1);
    }
}
