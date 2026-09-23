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

package io.sophiadata.flink.sync.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import io.sophiadata.flink.compat.ParameterTool;
import io.sophiadata.flink.sync.table.CustomDebeziumDeserializer;
import io.sophiadata.flink.sync.util.ParameterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Builds the MySQL CDC source stream.
 *
 * <p>Uses Flink CDC 3.x directly ({@code org.apache.flink.cdc.*}). Both supported version lines
 * (Flink 1.20 and Flink 2.2) ship CDC 3.x with this same package, so one implementation works on
 * either line; only the dependency version changes.
 */
public class MysqlCdcSource {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlCdcSource.class);

    public SingleOutputStreamOperator<Tuple2<String, Row>> singleOutputStreamOperator(
            ParameterTool params,
            StreamExecutionEnvironment env,
            Map<String, RowType> tableRowTypeMap) {

        String databaseName = ParameterUtil.databaseName(params);
        String tableList = ParameterUtil.tableList(params);

        LOG.info(
                "Building MySQL CDC source for database '{}' tables '{}'", databaseName, tableList);

        MySqlSource<Tuple2<String, Row>> mySqlSource =
                MySqlSource.<Tuple2<String, Row>>builder()
                        .hostname(ParameterUtil.hostname(params))
                        .port(ParameterUtil.port(params))
                        .databaseList(databaseName)
                        // A bare table name is qualified with the database, and `.*` is expanded.
                        .tableList(ParameterUtil.normalizeTableList(databaseName, tableList))
                        .username(ParameterUtil.username(params))
                        .password(ParameterUtil.password(params))
                        .deserializer(new CustomDebeziumDeserializer(tableRowTypeMap))
                        .startupOptions(StartupOptions.initial())
                        .build();

        return env.fromSource(
                        mySqlSource,
                        WatermarkStrategy.noWatermarks(),
                        ParameterUtil.cdcSourceName(params))
                .disableChaining()
                .setParallelism(ParameterUtil.setParallelism(params));
    }
}
