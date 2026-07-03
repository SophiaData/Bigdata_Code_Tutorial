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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import io.sophiadata.flink.sync.base.BaseCode;
import io.sophiadata.flink.sync.schema.SchemaEvolver;
import io.sophiadata.flink.sync.util.MysqlUtil;
import io.sophiadata.flink.sync.util.NacosUtil;
import io.sophiadata.flink.sync.util.ParameterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * MySQL 整库实时同步示例 —— 基于 flink-cdc 3.x 的 MySqlSource。
 *
 * <p>数据流：MySQL binlog → {@link CdcEventDeserializer} → {@code schema-evolver}（处理 DDL 变更） → {@link
 * CDBBatchSink}（批量写入 sink MySQL）。
 *
 * <p>同步流程分两阶段：
 *
 * <ol>
 *   <li><b>Bootstrap</b>（启动时）：通过 JDBC 读取源端 information_schema，为每张表在 sink 端 建好对应的 {@code
 *       sink_<table>} 表，并缓存列信息和主键。
 *   <li><b>CDC 追赶</b>（运行时）：从 MySQL binlog 实时捕获增量变更，通过 {@link CDBBatchSink} 以 upsert / delete 方式写入
 *       sink 表。DDL 变更（建表、加列） 由 {@link io.sophiadata.flink.sync.schema.SchemaEvolver} 自动同步到 sink 端。
 * </ol>
 */
public class FlinkSqlWDS extends BaseCode {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlWDS.class);
    private static final int BATCH_SIZE = 200;
    private static final long BATCH_INTERVAL_MS = 1000;

    public static void main(final String[] args) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        final String[] effectiveArgs = (args.length == 0) ? findDefaultArgs() : args;
        final String[] mergedArgs =
                NacosUtil.mergeInto(ParameterTool.fromArgs(effectiveArgs))
                        .toMap()
                        .entrySet()
                        .stream()
                        .flatMap(e -> java.util.stream.Stream.of("--" + e.getKey(), e.getValue()))
                        .toArray(String[]::new);
        new FlinkSqlWDS().init(mergedArgs, "flink_sql_wds", true, true);
    }

    private static String[] findDefaultArgs() {
        final String configPath = findDefaultConfig();
        return (configPath != null) ? new String[] {"--config", configPath} : new String[0];
    }

    private static String findDefaultConfig() {
        try {
            if (FlinkSqlWDS.class.getResource("/config.properties") != null) {
                return "classpath:config.properties";
            }
        } catch (Exception e) {
            LOG.debug("config.properties not on classpath, trying file paths");
        }
        final java.nio.file.Path l = java.nio.file.Paths.get("config.properties");
        if (java.nio.file.Files.exists(l)) {
            return l.toAbsolutePath().toString();
        }
        final java.nio.file.Path m = java.nio.file.Paths.get("cdc-mysql-sync", "config.properties");
        if (java.nio.file.Files.exists(m)) {
            return m.toAbsolutePath().toString();
        }
        return null;
    }

    /**
     * 核心管道组装。读取参数 → 构建 CDC Source → 挂载 schema 变更处理 → 接入批量 JDBC Sink。 注意：CDC source 的并行度必须为 1，否则无法保证
     * binlog 事件的全局有序性。
     */
    @Override
    public void handle(
            final String[] args,
            final StreamExecutionEnvironment env,
            final org.apache.flink.table.api.bridge.java.StreamTableEnvironment tEnv)
            throws Exception {

        // 1. 解析连接参数（host / port / username / password / database / sinkUrl 等）
        final ParameterTool p = ParameterTool.fromArgs(args);
        final String h = ParameterUtil.hostname(p);
        final int port = ParameterUtil.port(p);
        final String u = ParameterUtil.username(p);
        final String pw = ParameterUtil.password(p);
        final String db = ParameterUtil.databaseName(p);
        final String tz = p.get("serverTimeZone", "Asia/Shanghai");
        final String su = ParameterUtil.sinkUrl(p);
        final String sku = ParameterUtil.sinkUsername(p);
        final String skp = ParameterUtil.sinkPassword(p);

        final String sinkJdbcUrl = extractSinkJdbcUrl(su);

        // schemas: tableName → {columnName → cdcType}，用于动态建表和生成 INSERT SQL
        // pks:      tableName → 主键列名，用于生成 DELETE SQL 和 ON DUPLICATE KEY
        // Using static holder to bypass Flink serialization — operators get separate
        // copies of local ConcurrentHashMap, breaking DDL-triggered schema updates.
        final Map<String, Map<String, String>> schemas = SharedSchemaState.schemas();
        final Map<String, String> pks = SharedSchemaState.pks();

        final SchemaEvolver schemaEvolver = new SchemaEvolver(sinkJdbcUrl, sku, skp, "sink_");
        LOG.info("SchemaEvolver initialized for sink: {}", sinkJdbcUrl);

        // 构建 CDC Source：监听整个数据库（db + ".*" 匹配所有表）
        // includeSchemaChanges(true) 让 DDL 事件（建表、加列）也能流入管道
        final MySqlSource<Event> source =
                MySqlSource.<Event>builder()
                        .hostname(h)
                        .port(port)
                        .username(u)
                        .password(pw)
                        .databaseList(db)
                        .tableList(db + ".*")
                        .serverTimeZone(tz)
                        .deserializer(new CdcEventDeserializer())
                        .includeSchemaChanges(true)
                        .build();

        // 并行度 = 1：binlog 是全局有序的，多并行度会导致事件乱序
        final DataStream<Event> cdcStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "mysql-cdc-all")
                        .setParallelism(1);

        final String finalSinkJdbcUrl = sinkJdbcUrl;
        final String finalSku = sku;
        final String finalSkp = skp;
        final SchemaEvolver finalEvolver = schemaEvolver;

        // 在数据流中拦截 DDL 事件：建表时同步到 sink 端，加列时更新 schemas 缓存
        // 所有事件（DDL + DML）都原样传递给下游 Sink
        cdcStream
                .process(
                        new ProcessFunction<Event, Event>() {
                            @Override
                            public void processElement(
                                    final Event event,
                                    final Context ctx,
                                    final Collector<Event> out) {
                                handleSchemaEvent(
                                        event,
                                        finalEvolver,
                                        finalSinkJdbcUrl,
                                        finalSku,
                                        finalSkp,
                                        schemas,
                                        pks);
                                out.collect(event);
                            }
                        })
                .name("schema-evolver")
                .uid("schema-evolver")
                .addSink(
                        new CDBBatchSink(
                                finalSinkJdbcUrl,
                                finalSku,
                                finalSkp,
                                BATCH_SIZE,
                                BATCH_INTERVAL_MS));

        LOG.info("CDC pipeline with SchemaEvolver ready for database {}", db);

        // Bootstrap：在 CDC 追赶之前，先通过 JDBC 读取源端元数据，
        // 为已有的表在 sink 端建好对应的 sink_<table> 表
        final SourceConfig srcConfig = new SourceConfig(h, port, u, pw, db);
        bootstrapSinkTables(srcConfig, finalSinkJdbcUrl, finalSku, finalSkp, schemas, pks);

        env.execute("flink-cdc-wds-" + db);
    }

    private static void handleSchemaEvent(
            final Event event,
            final SchemaEvolver evolver,
            final String sinkJdbcUrl,
            final String sinkUser,
            final String sinkPassword,
            final Map<String, Map<String, String>> schemas,
            final Map<String, String> pks) {
        if (!(event instanceof SchemaChangeEvent)) {
            return;
        }
        try {
            evolver.processEvent((SchemaChangeEvent) event);
        } catch (Exception e) {
            LOG.error("SchemaEvolver error", e);
        }
        if (event instanceof CreateTableEvent) {
            handleCreateTable(
                    (CreateTableEvent) event, sinkJdbcUrl, sinkUser, sinkPassword, schemas, pks);
        } else if (event instanceof AddColumnEvent) {
            handleAddColumn((AddColumnEvent) event, schemas);
        } else if (event instanceof DropColumnEvent) {
            handleDropColumn((DropColumnEvent) event, schemas);
        }
    }

    private static void handleCreateTable(
            final CreateTableEvent cte,
            final String sinkJdbcUrl,
            final String sinkUser,
            final String sinkPassword,
            final Map<String, Map<String, String>> schemas,
            final Map<String, String> pks) {
        final String tableName = cte.tableId().getTableName();
        final Map<String, String> colMap = new LinkedHashMap<>();
        for (final Column col : cte.getSchema().getColumns()) {
            colMap.put(col.getName(), col.getType().toString());
        }
        schemas.put(tableName, colMap);
        final String pk =
                cte.getSchema().primaryKeys().isEmpty()
                        ? null
                        : cte.getSchema().primaryKeys().get(0);
        pks.put(tableName, pk);
        MysqlUtil.createSinkTableIfNotExists(
                sinkJdbcUrl, sinkUser, sinkPassword, tableName, colMap, pk);
        LOG.info("CreateTable schema stored for {}", tableName);
    }

    private static void handleAddColumn(
            final AddColumnEvent ace, final Map<String, Map<String, String>> schemas) {
        final String tableName = ace.tableId().getTableName();
        final Map<String, String> colMap = schemas.get(tableName);
        if (colMap == null) {
            return;
        }
        for (final AddColumnEvent.ColumnWithPosition cp : ace.getAddedColumns()) {
            colMap.put(cp.getAddColumn().getName(), cp.getAddColumn().getType().toString());
        }
        LOG.info("AddColumn schema updated for {}", tableName);
    }

    private static void handleDropColumn(
            final DropColumnEvent dce, final Map<String, Map<String, String>> schemas) {
        final String tableName = dce.tableId().getTableName();
        final Map<String, String> colMap = schemas.get(tableName);
        if (colMap == null) {
            return;
        }
        for (final String droppedCol : dce.getDroppedColumnNames()) {
            colMap.remove(droppedCol);
        }
        LOG.info(
                "DropColumn schema updated for {}: removed {}",
                tableName,
                dce.getDroppedColumnNames());
    }

    /**
     * 通过 JDBC 直连源端 MySQL，读取 information_schema 获取每张表的列信息和主键， 然后在 sink 端创建对应的 {@code sink_<table>}
     * 表，并将元数据写入 schemas / pks 缓存。 这样 CDC 流启动后，即使还没有收到 CreateTableEvent，sink 表也已经就绪。
     */
    private static void bootstrapSinkTables(
            final SourceConfig source,
            final String sinkJdbcUrl,
            final String sinkUser,
            final String sinkPw,
            final Map<String, Map<String, String>> schemas,
            final Map<String, String> pks)
            throws SQLException, ClassNotFoundException {
        final String sourceUrl =
                String.format(
                        "jdbc:mysql://%s:%d?useSSL=false&allowPublicKeyRetrieval=true",
                        source.host, source.port);
        try (Connection conn = MysqlUtil.getConnection(sourceUrl, source.user, source.password)) {
            Class.forName("com.mysql.cj.jdbc.Driver");
            final String validatedDb = MysqlUtil.validateIdentifier(source.database);
            String[] tables;
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SHOW TABLES FROM `" + validatedDb + "`")) {
                final List<String> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(rs.getString(1));
                }
                tables = list.toArray(new String[0]);
            }
            LOG.info(
                    "Bootstrapping {} source tables from {}: {}",
                    tables.length,
                    source.database,
                    tables);

            for (final String table : tables) {
                final Map<String, String> cols = new LinkedHashMap<>();
                String pk = null;
                try (PreparedStatement psCol =
                        conn.prepareStatement(
                                "SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")) {
                    psCol.setString(1, source.database);
                    psCol.setString(2, table);
                    try (ResultSet rs = psCol.executeQuery()) {
                        while (rs.next()) {
                            cols.put(rs.getString("COLUMN_NAME"), rs.getString("DATA_TYPE"));
                        }
                    }
                }
                try (PreparedStatement psPk =
                        conn.prepareStatement(
                                "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'")) {
                    psPk.setString(1, source.database);
                    psPk.setString(2, table);
                    try (ResultSet rs = psPk.executeQuery()) {
                        if (rs.next()) {
                            pk = rs.getString(1);
                        }
                    }
                }
                if (cols.isEmpty()) {
                    LOG.warn("Skipping empty schema for {}.{}", source.database, table);
                    continue;
                }
                final String sinkTable = "sink_" + table;
                schemas.put(table, cols);
                pks.put(table, pk);
                MysqlUtil.createSinkTableIfNotExists(
                        sinkJdbcUrl, sinkUser, sinkPw, sinkTable, cols, pk);
                LOG.info(
                        "Bootstrapped sink for {}.{} -> {} (pk={})",
                        source.database,
                        table,
                        sinkTable,
                        pk);
            }
        }
    }

    static class SourceConfig {
        final String host;
        final int port;
        final String user;
        final String password;
        final String database;

        SourceConfig(
                final String host,
                final int port,
                final String user,
                final String password,
                final String database) {
            this.host = host;
            this.port = port;
            this.user = user;
            this.password = password;
            this.database = database;
        }
    }

    private static String extractSinkJdbcUrl(final String su) {
        final int dbStart = su.indexOf("//") + 2;
        final int dbEnd = su.indexOf("/", dbStart);
        final String hostPort = su.substring(dbStart, dbEnd);
        final int paramStart = su.indexOf("?");
        final String database = su.substring(dbEnd + 1, paramStart > 0 ? paramStart : su.length());
        return "jdbc:mysql://" + hostPort + "/" + database;
    }
}
