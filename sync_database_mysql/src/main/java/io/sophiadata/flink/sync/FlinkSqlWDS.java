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

package io.sophiadata.flink.sync;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import io.sophiadata.flink.compat.ParameterTool;
import io.sophiadata.flink.sync.base.BaseCode;
import io.sophiadata.flink.sync.sink.CreateMysqlLSinkTable;
import io.sophiadata.flink.sync.source.MysqlCdcSource;
import io.sophiadata.flink.sync.util.MysqlSchemaReader;
import io.sophiadata.flink.sync.util.MysqlTypeMapper;
import io.sophiadata.flink.sync.util.MysqlUtil;
import io.sophiadata.flink.sync.util.ParameterUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Whole-database MySQL to MySQL synchronization.
 *
 * <p>Detects the source schema over JDBC, creates a matching Flink JDBC sink table per source
 * table, and streams changes through Flink CDC.
 *
 * <p>(@SophiaData) (@date 2023/5/31 19:09).
 */
public class FlinkSqlWDS extends BaseCode {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlWDS.class);

    public static void main(String[] args) throws Exception {
        new FlinkSqlWDS().init(args, "flink_sql_job_FlinkSqlWDS", true, true);
        LOG.info(" init 方法正常 ");
    }

    // 本程序测试 Whole database synchronization 之 MySQL to MySQL 捕捉表需包含主键并实现自动建表，DDL 同步暂不支持 ！！！
    // 可根据此案例拓展其他 sink 组件
    // 需要注意的点：不同表数据量不一样，同步时可以适当放大同步资源，但会造成资源浪费，不加大可能反压
    // 测试同步五张表百万数据，一分钟左右
    // refer: https://blog.csdn.net/qq_36062467/article/details/128117647
    // 本程序环境：Flink 1.17.1 / 1.20.0 + Flink CDC 2.4.x / 3.x  MySQL 8.0
    // 技术点：Flink MySQL CDC Connector，JDBC Catalog 元数据，Flink Operator，Flink JDBC

    @Override
    public void handle(String[] args, StreamExecutionEnvironment env, StreamTableEnvironment tEnv)
            throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        // setGlobalJobParameters only accepts the real Flink type (its Map overload is private), so
        // hand it the underlying ParameterTool, which extends GlobalJobParameters on both lines.
        env.getConfig().setGlobalJobParameters(params.asGlobalJobParameters());
        String databaseName = ParameterUtil.databaseName(params);
        String tableList = ParameterUtil.tableList(params);
        String sinkPrefix = ParameterUtil.sinkPrefix(params);

        String connectorWithBody = CreateMysqlLSinkTable.connectorWithBody(params);

        List<String> tables;
        try (Connection sourceConnection =
                MysqlUtil.getConnection(
                        MysqlUtil.sourceJdbcUrl(params),
                        ParameterUtil.username(params),
                        ParameterUtil.password(params))) {

            tables = resolveTables(sourceConnection, databaseName, tableList);

            Map<String, RowTypeInfo> tableTypeInformationMap = new LinkedHashMap<>();
            Map<String, RowType> tableRowTypeMap = new LinkedHashMap<>();

            for (String table : tables) {
                MysqlSchemaReader.TableSchema tableSchema =
                        MysqlSchemaReader.readTableSchema(
                                sourceConnection, databaseName, table, MysqlTypeMapper.standard());

                validatePrimaryKey(table, tableSchema.primaryKeys());

                // The same sink name must be used both when creating the sink table and when
                // inserting into it. Previously the insert path hardcoded "sink_%s" while the
                // create path honoured a configurable prefix, so any custom sinkPrefix produced
                // a table that the INSERT could not find.
                String sinkTableName = String.format(sinkPrefix, table);

                registerSinkTable(tEnv, connectorWithBody, sinkTableName, tableSchema, sinkPrefix);

                tableRowTypeMap.put(table, tableSchema.rowType());

                TypeInformation<?>[] fieldTypes =
                        new TypeInformation[tableSchema.fieldNames().length];
                for (int i = 0; i < tableSchema.fieldDataTypes().length; i++) {
                    fieldTypes[i] =
                            InternalTypeInfo.of(tableSchema.fieldDataTypes()[i].getLogicalType());
                }
                tableTypeInformationMap.put(
                        table, new RowTypeInfo(fieldTypes, tableSchema.fieldNames()));

                // 下游 MySQL 建表逻辑
                new CreateMysqlLSinkTable()
                        .createMysqlSinkTable(
                                params,
                                sinkTableName,
                                tableSchema.fieldNames(),
                                tableSchema.fieldDataTypes(),
                                tableSchema.primaryKeys());
            }

            if (tables.isEmpty()) {
                throw new IllegalStateException(
                        "No tables selected for synchronization in database '"
                                + databaseName
                                + "'");
            }

            buildAndExecutePipeline(
                    params,
                    env,
                    tEnv,
                    connectorWithBody,
                    sinkPrefix,
                    tableTypeInformationMap,
                    tableRowTypeMap);
        }
    }

    /** Resolves the configured table list into concrete table names. */
    private List<String> resolveTables(
            Connection sourceConnection, String databaseName, String tableList)
            throws SQLException {
        if (".*".equals(tableList)) {
            return MysqlSchemaReader.listTables(sourceConnection, databaseName);
        }
        if (tableList.contains(",")) {
            return extractTableNames(tableList);
        }
        return new ArrayList<>(java.util.Collections.singletonList(tableList));
    }

    /** Fails fast when a source table has no primary key, which MySQL CDC cannot capture. */
    private void validatePrimaryKey(String table, List<String> primaryKeys) {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new IllegalStateException(
                    "Table '"
                            + table
                            + "' has no primary key. MySQL CDC does not support capturing tables "
                            + "without a primary key.");
        }
    }

    /** Creates the Flink JDBC sink table for one source table. */
    private void registerSinkTable(
            StreamTableEnvironment tEnv,
            String connectorWithBody,
            String sinkTableName,
            MysqlSchemaReader.TableSchema tableSchema,
            String sinkPrefix) {

        String[] fieldNames = tableSchema.fieldNames();
        DataType[] fieldDataTypes = tableSchema.fieldDataTypes();

        StringBuilder stmt = new StringBuilder();
        stmt.append("create table if not exists ").append(sinkTableName).append("(\n");
        for (int i = 0; i < fieldNames.length; i++) {
            stmt.append("\t`")
                    .append(fieldNames[i])
                    .append("` ")
                    .append(fieldDataTypes[i].toString())
                    .append(",\n");
        }
        stmt.append(
                String.format(
                        "PRIMARY KEY (%s) NOT ENFORCED\n)",
                        StringUtils.join(
                                tableSchema.primaryKeys().stream()
                                        .map(k -> "`" + k + "`")
                                        .collect(Collectors.toList()),
                                ",")));

        String createSinkTableDdl =
                stmt + connectorWithBody.replace("${sinkTableName}", sinkTableName);
        LOG.info("Creating sink table:\n{}", createSinkTableDdl);
        tEnv.executeSql(createSinkTableDdl);
    }

    /** Wires the CDC source into the sink tables and executes the job. */
    private void buildAndExecutePipeline(
            ParameterTool params,
            StreamExecutionEnvironment env,
            StreamTableEnvironment tEnv,
            String connectorWithBody,
            String sinkPrefix,
            Map<String, RowTypeInfo> tableTypeInformationMap,
            Map<String, RowType> tableRowTypeMap)
            throws Exception {

        SingleOutputStreamOperator<Tuple2<String, Row>> dataStreamSource =
                new MysqlCdcSource().singleOutputStreamOperator(params, env, tableRowTypeMap);

        StatementSet statementSet = tEnv.createStatementSet();
        for (Map.Entry<String, RowTypeInfo> entry : tableTypeInformationMap.entrySet()) {
            String tableName = entry.getKey();
            RowTypeInfo rowTypeInfo = entry.getValue();

            SingleOutputStreamOperator<Row> mapStream =
                    dataStreamSource
                            .filter(data -> data.f0.equals(tableName))
                            .setParallelism(ParameterUtil.setParallelism(params))
                            .map(data -> data.f1, rowTypeInfo)
                            .setParallelism(ParameterUtil.setParallelism(params));

            Table table = tEnv.fromChangelogStream(mapStream);
            String temporaryViewName = String.format("t_%s", tableName);
            tEnv.createTemporaryView(temporaryViewName, table);

            // Must match the name used by registerSinkTable().
            String sinkTableName = String.format(sinkPrefix, tableName);
            String insertSql =
                    String.format(
                            "insert into %s select * from %s", sinkTableName, temporaryViewName);
            LOG.info("Adding insert statement for {}: {}", tableName, insertSql);
            statementSet.addInsertSql(insertSql);
        }
        statementSet.execute();
    }

    /** 提取方法：从逗号分隔的表格列表中提取表格名称 */
    private List<String> extractTableNames(String tableList) {
        return Arrays.stream(tableList.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(table -> table.contains(".") ? table.substring(table.indexOf('.') + 1) : table)
                .collect(Collectors.toList());
    }
}
