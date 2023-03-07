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

package io.sophiadata.flink.cdc2;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.catalog.MySqlCatalog;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DefaultCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;

import io.sophiadata.flink.base.BaseSql;
import io.sophiadata.flink.cdc2.sink.CreateMySQLSinkTable;
import io.sophiadata.flink.cdc2.source.MySQLCDCSource;
import io.sophiadata.flink.cdc2.util.MySQLUtil;
import io.sophiadata.flink.cdc2.util.ParameterUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** (@SophiaData) (@date 2022/10/25 10:56). */
public class FlinkSqlWDS extends BaseSql {
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
    // refer 环境: Flink 1.15 Flink CDC 2.3.0
    // 本程序环境：Flink 1.16 Flink CDC 2.3.0  MySQL 8.0
    // 技术点：Flink MySQL CDC Connector，MySQL Catalog，Flink Operator，Flink JDBC

    @Override
    public void handle(
            StreamExecutionEnvironment env, StreamTableEnvironment tEnv, ParameterTool params)
            throws Exception {
        String databaseName = ParameterUtil.databaseName(params);
        String tableList = ParameterUtil.tableList(params);

        String connectorWithBody = CreateMySQLSinkTable.connectorWithBody(params);

        // 注册同步的库对应的 Catalog 这里是 mysql catalog

        MySqlCatalog mySqlCatalog = MySQLUtil.useMysqlCatalog(params);

        List<String> tables;

        // 如果整库同步，则从 Catalog 里取所有表，否则从指定表中取表名
        try {
            if (".*".equals(tableList)) {
                tables = mySqlCatalog.listTables(databaseName);
            } else {
                if (tableList.contains(",")) {
                    String[] tableArray = tableList.split(",");
                    tables =
                            Arrays.stream(tableArray)
                                    .map(table -> table.split("\\.")[1])
                                    .collect(Collectors.toList());
                } else {
                    tables = Collections.singletonList(tableList);
                }
            }
        } catch (DatabaseNotExistException e) {
            LOG.error("{} 库不存在", databaseName, e);
            throw e;
        }
        // 创建表名和对应 RowTypeInfo 映射的 Map
        Map<String, RowTypeInfo> tableTypeInformationMap = Maps.newConcurrentMap();
        Map<String, DataType[]> tableDataTypesMap = Maps.newConcurrentMap();
        Map<String, RowType> tableRowTypeMap = Maps.newConcurrentMap();
        for (String table : tables) {
            // 获取  Catalog 中注册的表
            ObjectPath objectPath = new ObjectPath(databaseName, table);
            DefaultCatalogTable catalogBaseTable;
            try {
                catalogBaseTable = (DefaultCatalogTable) mySqlCatalog.getTable(objectPath);
            } catch (TableNotExistException e) {
                LOG.error("{} 表不存在", table, e);
                throw e;
            }
            // 获取表的 Schema
            assert catalogBaseTable != null;
            Schema schema = catalogBaseTable.getUnresolvedSchema();
            // 获取表中字段名列表
            String[] fieldNames = new String[schema.getColumns().size()];
            // 获取DataType
            DataType[] fieldDataTypes = new DataType[schema.getColumns().size()];
            LogicalType[] logicalTypes = new LogicalType[schema.getColumns().size()];

            // 获取表字段类型
            TypeInformation<?>[] fieldTypes = new TypeInformation[schema.getColumns().size()];
            // 获取表的主键
            List<String> primaryKeys;
            try {
                primaryKeys = schema.getPrimaryKey().get().getColumnNames(); // 此处不用 orElse
            } catch (NullPointerException e) {
                LOG.error("捕捉表异常: {} 表没有主键！！！ 当前 mysql cdc 尚不支持捕捉没有主键的表！！！", table, e);
                throw e;
            }

            for (int i = 0; i < schema.getColumns().size(); i++) {
                Schema.UnresolvedPhysicalColumn column =
                        (Schema.UnresolvedPhysicalColumn) schema.getColumns().get(i);
                fieldNames[i] = column.getName();
                fieldDataTypes[i] = (DataType) column.getDataType();
                fieldTypes[i] =
                        InternalTypeInfo.of(((DataType) column.getDataType()).getLogicalType());
                logicalTypes[i] = ((DataType) column.getDataType()).getLogicalType();
            }
            RowType rowType = RowType.of(logicalTypes, fieldNames);
            tableRowTypeMap.put(table, rowType);

            // 组装 Flink Sink 表 DDL sql
            StringBuilder stmt = new StringBuilder();
            String sinkTableName =
                    String.format(params.get("sinkPrefix", "sink_%s"), table); // Sink 表前缀
            stmt.append("create table if not exists ").append(sinkTableName).append("(\n");

            for (int i = 0; i < fieldNames.length; i++) {
                String column = fieldNames[i];
                String fieldDataType = fieldDataTypes[i].toString();
                stmt.append("\t`").append(column).append("` ").append(fieldDataType).append(",\n");
            }

            stmt.append(
                    String.format(
                            "PRIMARY KEY (%s) NOT ENFORCED\n)",
                            StringUtils.join(primaryKeys, ",")));
            String formatJdbcSinkWithBody =
                    connectorWithBody.replace("${sinkTableName}", sinkTableName);
            String createSinkTableDdl = stmt + formatJdbcSinkWithBody;
            // 创建 Flink Sink 表
            LOG.info("createSinkTableDdl: \r {}", createSinkTableDdl);
            tEnv.executeSql(createSinkTableDdl);
            tableDataTypesMap.put(table, fieldDataTypes);
            tableTypeInformationMap.put(table, new RowTypeInfo(fieldTypes, fieldNames));

            // 下游 MySQL 建表逻辑
            new CreateMySQLSinkTable()
                    .createMySQLSinkTable(
                            params, sinkTableName, fieldNames, fieldDataTypes, primaryKeys);
        }

        //  MySQL CDC
        SingleOutputStreamOperator<Tuple2<String, Row>> dataStreamSource =
                new MySQLCDCSource()
                        .singleOutputStreamOperator(params, env, tableRowTypeMap); // 切断任务链
        StatementSet statementSet = tEnv.createStatementSet();
        // DataStream 转 Table，创建临时视图，插入 sink 表
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
            String sinkTableName = String.format("sink_%s", tableName);
            String insertSql =
                    String.format(
                            "insert into %s select * from %s", sinkTableName, temporaryViewName);
            LOG.info("add insertSql for {}, sql: {}", tableName, insertSql);
            statementSet.addInsertSql(insertSql);
        }
        statementSet.execute();
    }
}
