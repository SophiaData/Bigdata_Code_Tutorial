package io.sophiadata.flink.cdc2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.catalog.MySqlCatalog;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
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

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import io.sophiadata.flink.base.BaseSql;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** (@SophiaData) (@date 2022/10/25 10:56). */
public class FlinkSqlWDS2 extends BaseSql {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlWDS2.class);

    public static void main(String[] args) {
        new FlinkSqlWDS2().init(args, "flink_sql_job_FlinkSqlWDS2", true, true);
        LOG.info(" init 方法正常 ");
    }

    // 本程序测试 Whole database synchronization 捕捉表需包含主键 后续待实现自动建表，自动同步表结构等功能
    // refer: https://blog.csdn.net/qq_36062467/article/details/128117647  环境：Flink 1.15 CDC 2.3
    // 环境：Flink 1.16 CDC 2.3

    @Override
    public void handle(
            StreamExecutionEnvironment env, StreamTableEnvironment tEnv, ParameterTool params) {
        String hostname = params.get("hostname", "localhost");
        int port = params.getInt("port", 3306);
        String username = params.get("username", "root");
        String password = params.get("password", "123456");
        String databaseName = params.get("databaseName", "test");
        String tableList = params.get("tableList", "test.test3");

        String sinkUrl =
                "jdbc:mysql://localhost:3306/test2?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
        String sinkUsername = "root";
        String sinkPassword = "123456";

        String connectorWithBody =
                " with (\n"
                        + " 'connector' = 'jdbc',\n"
                        + " 'url' = '${sinkUrl}',\n"
                        + " 'username' = '${sinkUsername}',\n"
                        + " 'password' = '${sinkPassword}',\n"
                        + " 'table-name' = '${tableName}'\n"
                        + ")";

        connectorWithBody =
                connectorWithBody
                        .replace("${sinkUrl}", sinkUrl)
                        .replace("${sinkUsername}", sinkUsername)
                        .replace("${sinkPassword}", sinkPassword);

        // 注册同步的库对应的catalog

        MySqlCatalog mysqlCatalog =
                new MySqlCatalog(
                        Thread.currentThread().getContextClassLoader(), // MySQL 8 驱动
                        // Class.forName("com.mysql.cj.jdbc.Driver").getClassLoader()
                        "mysql-catalog",
                        databaseName,
                        username,
                        password,
                        String.format("jdbc:mysql://%s:%d", hostname, port));
        List<String> tables = new ArrayList<>();

        // 如果整库同步，则从catalog里取所有表，否则从指定表中取表名
        try {
            if (".*".equals(tableList)) {
                tables = mysqlCatalog.listTables(databaseName);
            } else {
                String[] tableArray = tableList.split(",");
                for (String table : tableArray) {
                    tables.add(table.split("\\.")[1]);
                }
            }
        } catch (DatabaseNotExistException e) {
            LOG.error("{} 库不存在", databaseName, e);
        }
        // 创建表名和对应 RowTypeInfo 映射的 Map
        Map<String, RowTypeInfo> tableTypeInformationMap = Maps.newConcurrentMap();
        Map<String, DataType[]> tableDataTypesMap = Maps.newConcurrentMap();
        Map<String, RowType> tableRowTypeMap = Maps.newConcurrentMap();
        for (String table : tables) {
            // 获取 MySQL Catalog 中注册的表
            ObjectPath objectPath = new ObjectPath(databaseName, table);
            DefaultCatalogTable catalogBaseTable = null;
            try {
                catalogBaseTable = (DefaultCatalogTable) mysqlCatalog.getTable(objectPath);
            } catch (TableNotExistException e) {
                LOG.error("{} 表不存在", table, e);
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
            List<String> primaryKeys = null;
            try {
                primaryKeys = schema.getPrimaryKey().get().getColumnNames();
            } catch (NullPointerException e) {
                LOG.error("捕捉表异常: {} 没有主键", table, e);
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
            String jdbcSinkTableName = String.format("jdbc_sink_%s", table); // Sink 表前缀
            stmt.append("create table if not exists ").append(jdbcSinkTableName).append("(\n");

            for (int i = 0; i < fieldNames.length; i++) {
                String column = fieldNames[i];
                String fieldDataType = fieldDataTypes[i].toString();
                stmt.append("\t").append(column).append(" ").append(fieldDataType).append(",\n");
            }
            stmt.append(
                    String.format(
                            "PRIMARY KEY (%s) NOT ENFORCED\n)",
                            StringUtils.join(primaryKeys, ",")));
            String formatJdbcSinkWithBody =
                    connectorWithBody.replace("${tableName}", jdbcSinkTableName);
            String createSinkTableDdl = stmt + formatJdbcSinkWithBody;
            // 创建 Flink sink 表
            LOG.info("createSinkTableDdl: \r {}", createSinkTableDdl);
            //            tEnv.executeSql(createSinkTableDdl);
            tableDataTypesMap.put(table, fieldDataTypes);
            tableTypeInformationMap.put(table, new RowTypeInfo(fieldTypes, fieldNames));
        }

        //  MySQL CDC
        MySqlSource<Tuple2<String, Row>> mySqlSource =
                MySqlSource.<Tuple2<String, Row>>builder()
                        .hostname(hostname)
                        .port(port)
                        .databaseList(databaseName)
                        .tableList(tableList)
                        .username(username)
                        .password(password)
                        .deserializer(new CustomDebeziumDeserializer2(tableRowTypeMap))
                        .startupOptions(StartupOptions.initial())
                        .includeSchemaChanges(true)
                        .build();
        SingleOutputStreamOperator<Tuple2<String, Row>> dataStreamSource =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql cdc")
                        .disableChaining(); // 切断任务链
        StatementSet statementSet = tEnv.createStatementSet();
        // DataStream 转 Table，创建临时视图，插入 sink 表
        //        for (Map.Entry<String, RowTypeInfo> entry : tableTypeInformationMap.entrySet()) {
        //            String tableName = entry.getKey();
        //            RowTypeInfo rowTypeInfo = entry.getValue();
        dataStreamSource.print().setParallelism(1);
        //            SingleOutputStreamOperator<Row> mapStream =
        //                    dataStreamSource
        //                            .filter(data -> data.f0.equals(tableName))
        //                            .map(data -> data.f1, rowTypeInfo);
        //            Table table = tEnv.fromChangelogStream(mapStream);
        //            String temporaryViewName = String.format("t_%s", tableName);
        //            tEnv.createTemporaryView(temporaryViewName, table);
        //            String sinkTableName = String.format("jdbc_sink_%s", tableName);
        //            String insertSql =
        //                    String.format(
        //                            "insert into %s select * from %s", sinkTableName,
        // temporaryViewName);
        //            LOG.info("add insertSql for {},sql: {}", tableName, insertSql);
        //            statementSet.addInsertSql(insertSql);
        //        }
        //        statementSet.execute();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
