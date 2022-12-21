package io.sophiadata.flink.cdc2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.catalog.MySqlCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.DefaultCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import io.sophiadata.flink.cdc2.table.CustomDebeziumDeserializer;
import io.sophiadata.flink.testutis.UniqueDatabase;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/** (@sophiadata) (@date 2022/12/20 16:41). */
public class MySqlSourceExampleTest extends MySqlSourceTestBase {

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", "mysqluser", "mysqlpw");

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testConsumingAllEvents() throws Exception {
        inventoryDatabase.createAndInitialize();

        MySqlCatalog mySqlCatalog =
                new MySqlCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        "mysqlCatalog",
                        inventoryDatabase.getDatabaseName(),
                        inventoryDatabase.getUsername(),
                        inventoryDatabase.getPassword(),
                        String.format(
                                "jdbc:mysql://%s:%d",
                                MYSQL_CONTAINER.getHost(), MYSQL_CONTAINER.getDatabasePort()));

        List<String> tables;
        Map<String, RowType> tableRowTypeMap;
        tables = mySqlCatalog.listTables(inventoryDatabase.getDatabaseName());

        // 创建表名和对应 RowTypeInfo 映射的 Map
        tableRowTypeMap = Maps.newConcurrentMap();

        for (String table : tables) {
            // 获取 MySQL Catalog 中注册的表
            ObjectPath objectPath = new ObjectPath(inventoryDatabase.getDatabaseName(), table);
            DefaultCatalogTable catalogBaseTable;
            catalogBaseTable = (DefaultCatalogTable) mySqlCatalog.getTable(objectPath);
            // 获取表的 Schema
            assert catalogBaseTable != null;
            Schema schema = catalogBaseTable.getUnresolvedSchema();
            // 获取表中字段名列表
            String[] fieldNames = new String[schema.getColumns().size()];
            // 获取DataType
            LogicalType[] logicalTypes = new LogicalType[schema.getColumns().size()];

            for (int i = 0; i < schema.getColumns().size(); i++) {
                Schema.UnresolvedPhysicalColumn column =
                        (Schema.UnresolvedPhysicalColumn) schema.getColumns().get(i);
                fieldNames[i] = column.getName();
                logicalTypes[i] = ((DataType) column.getDataType()).getLogicalType();
            }
            RowType rowType = RowType.of(logicalTypes, fieldNames);
            tableRowTypeMap.put(table, rowType);
            System.out.println("元数据信息：" + tableRowTypeMap); // 输出捕获表的元数据信息
        }

        MySqlSource<Tuple2<String, Row>> mySqlSource =
                MySqlSource.<Tuple2<String, Row>>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + ".*")
                        .username(inventoryDatabase.getUsername())
                        .password(inventoryDatabase.getPassword())
                        .serverId("5401-5404")
                        .serverTimeZone("Asia/Shanghai")
                        .deserializer(new CustomDebeziumDeserializer(tableRowTypeMap))
                        .includeSchemaChanges(false) // output the schema changes as well
                        .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 4
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlParallelSource")
                .setParallelism(4)
                .print("MySqlSourceExampleTest: ")
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
