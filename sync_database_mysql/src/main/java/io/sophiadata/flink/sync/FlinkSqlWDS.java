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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import io.sophiadata.flink.sync.base.BaseCode;
import io.sophiadata.flink.sync.schema.SchemaEvolver;
import io.sophiadata.flink.sync.util.MysqlUtil;
import io.sophiadata.flink.sync.util.NacosUtil;
import io.sophiadata.flink.sync.util.ParameterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Whole-Database Sync main class using flink-cdc 3.x MySqlSource. */
public class FlinkSqlWDS extends BaseCode {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlWDS.class);
    private static final int BATCH_SIZE = 200;
    private static final long BATCH_INTERVAL_MS = 1000;

    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        String[] effectiveArgs = (args.length == 0) ? findDefaultArgs() : args;
        String[] mergedArgs =
                NacosUtil.mergeInto(ParameterTool.fromArgs(effectiveArgs))
                        .toMap()
                        .entrySet()
                        .stream()
                        .flatMap(e -> java.util.stream.Stream.of("--" + e.getKey(), e.getValue()))
                        .toArray(String[]::new);
        new FlinkSqlWDS().init(mergedArgs, "flink_sql_wds", true, true);
    }

    private static String[] findDefaultArgs() {
        String configPath = findDefaultConfig();
        return (configPath != null) ? new String[] {"--config", configPath} : new String[0];
    }

    private static String findDefaultConfig() {
        try {
            if (FlinkSqlWDS.class.getResource("/config.properties") != null) {
                return "classpath:config.properties";
            }
        } catch (Exception e) {
            // Ignore
        }
        java.nio.file.Path l = java.nio.file.Paths.get("config.properties");
        if (java.nio.file.Files.exists(l)) {
            return l.toAbsolutePath().toString();
        }
        java.nio.file.Path m = java.nio.file.Paths.get("sync_database_mysql", "config.properties");
        if (java.nio.file.Files.exists(m)) {
            return m.toAbsolutePath().toString();
        }
        return null;
    }

    @Override
    public void handle(
            String[] args,
            StreamExecutionEnvironment env,
            org.apache.flink.table.api.bridge.java.StreamTableEnvironment tEnv)
            throws Exception {

        ParameterTool p = ParameterTool.fromArgs(args);
        String h = ParameterUtil.hostname(p);
        int port = ParameterUtil.port(p);
        String u = ParameterUtil.username(p);
        String pw = ParameterUtil.password(p);
        String db = ParameterUtil.databaseName(p);
        String tz = p.get("serverTimeZone", "Asia/Shanghai");
        String su = ParameterUtil.sinkUrl(p);
        String sku = ParameterUtil.sinkUsername(p);
        String skp = ParameterUtil.sinkPassword(p);

        String sinkJdbcUrl = extractSinkJdbcUrl(su);

        Map<String, Map<String, String>> schemas = new ConcurrentHashMap<>();
        Map<String, String> pks = new ConcurrentHashMap<>();

        SchemaEvolver schemaEvolver = new SchemaEvolver(sinkJdbcUrl, sku, skp);
        LOG.info("SchemaEvolver initialized for sink: {}", sinkJdbcUrl);

        MySqlSource<Event> source =
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

        DataStream<Event> cdcStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "mysql-cdc-all")
                        .setParallelism(1);

        final String finalSinkJdbcUrl = sinkJdbcUrl;
        final String finalSku = sku;
        final String finalSkp = skp;
        final SchemaEvolver finalEvolver = schemaEvolver;

        cdcStream
                .process(
                        new ProcessFunction<Event, Event>() {
                            @Override
                            public void processElement(
                                    Event event, Context ctx, Collector<Event> out) {
                                if (event instanceof SchemaChangeEvent) {
                                    SchemaChangeEvent sce = (SchemaChangeEvent) event;
                                    try {
                                        finalEvolver.processEvent(sce);
                                    } catch (Exception e) {
                                        LOG.error("SchemaEvolver error: {}", e.getMessage());
                                    }
                                    if (event instanceof CreateTableEvent) {
                                        CreateTableEvent cte = (CreateTableEvent) event;
                                        String tableName = cte.tableId().getTableName();
                                        Map<String, String> colMap = new LinkedHashMap<>();
                                        for (Column col : cte.getSchema().getColumns()) {
                                            colMap.put(col.getName(), col.getType().toString());
                                        }
                                        schemas.put(tableName, colMap);
                                        String pk =
                                                cte.getSchema().primaryKeys().isEmpty()
                                                        ? "id"
                                                        : cte.getSchema().primaryKeys().get(0);
                                        pks.put(tableName, pk);
                                        createSinkTableIfNotExists(
                                                finalSinkJdbcUrl,
                                                finalSku,
                                                finalSkp,
                                                tableName,
                                                colMap,
                                                pk);
                                        LOG.info("CreateTable schema stored for {}", tableName);
                                    } else if (event instanceof AddColumnEvent) {
                                        AddColumnEvent ace = (AddColumnEvent) event;
                                        String tableName = ace.tableId().getTableName();
                                        Map<String, String> colMap = schemas.get(tableName);
                                        if (colMap != null) {
                                            for (AddColumnEvent.ColumnWithPosition cp :
                                                    ace.getAddedColumns()) {
                                                colMap.put(
                                                        cp.getAddColumn().getName(),
                                                        cp.getAddColumn().getType().toString());
                                            }
                                            LOG.info("AddColumn schema updated for {}", tableName);
                                        }
                                    }
                                }
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
                                BATCH_INTERVAL_MS,
                                schemas,
                                pks));

        LOG.info("CDC pipeline with SchemaEvolver ready for database {}", db);

        bootstrapSinkTables(h, port, u, pw, db, finalSinkJdbcUrl, finalSku, finalSkp, schemas, pks);

        env.execute("flink-cdc-wds-" + db);
    }

    private static void bootstrapSinkTables(
            String host,
            int port,
            String user,
            String pw,
            String db,
            String sinkJdbcUrl,
            String sinkUser,
            String sinkPw,
            Map<String, Map<String, String>> schemas,
            Map<String, String> pks)
            throws SQLException, ClassNotFoundException {
        String sourceUrl =
                String.format(
                        "jdbc:mysql://%s:%d?useSSL=false&allowPublicKeyRetrieval=true", host, port);
        try (Connection conn = MysqlUtil.getConnection(sourceUrl, user, pw)) {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String validatedDb = validateIdentifier(db);
            String[] tables;
            try (Statement st = conn.createStatement();
                    java.sql.ResultSet rs =
                            st.executeQuery("SHOW TABLES FROM `" + validatedDb + "`")) {
                List<String> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(rs.getString(1));
                }
                tables = list.toArray(new String[0]);
            }
            LOG.info("Bootstrapping {} source tables from {}: {}", tables.length, db, tables);

            for (String table : tables) {
                Map<String, String> cols = new LinkedHashMap<>();
                String pk = "id";
                try (PreparedStatement psCol =
                        conn.prepareStatement(
                                "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")) {
                    psCol.setString(1, db);
                    psCol.setString(2, table);
                    try (java.sql.ResultSet rs = psCol.executeQuery()) {
                        while (rs.next()) {
                            cols.put(rs.getString("COLUMN_NAME"), rs.getString("DATA_TYPE"));
                        }
                    }
                }
                try (PreparedStatement psPk =
                        conn.prepareStatement(
                                "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'")) {
                    psPk.setString(1, db);
                    psPk.setString(2, table);
                    try (java.sql.ResultSet rs = psPk.executeQuery()) {
                        if (rs.next()) {
                            pk = rs.getString(1);
                        }
                    }
                }
                if (cols.isEmpty()) {
                    LOG.warn("Skipping empty schema for {}.{}", db, table);
                    continue;
                }
                String sinkTable = "sink_" + table;
                schemas.put(table, cols);
                pks.put(table, pk);
                createSinkTableIfNotExists(sinkJdbcUrl, sinkUser, sinkPw, sinkTable, cols, pk);
                LOG.info("Bootstrapped sink for {}.{} -> {} (pk={})", db, table, sinkTable, pk);
            }
        }
    }

    private static String extractSinkJdbcUrl(String su) {
        int dbStart = su.indexOf("//") + 2;
        int dbEnd = su.indexOf("/", dbStart);
        String hostPort = su.substring(dbStart, dbEnd);
        int paramStart = su.indexOf("?");
        String database = su.substring(dbEnd + 1, paramStart > 0 ? paramStart : su.length());
        return "jdbc:mysql://" + hostPort + "/" + database;
    }

    private static final Pattern VALID_IDENTIFIER = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    private static String validateIdentifier(String identifier) {
        if (identifier == null || !VALID_IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Invalid SQL identifier: " + identifier);
        }
        return identifier;
    }

    public static class CdcEventDeserializer implements DebeziumDeserializationSchema<Event> {
        private static final Logger LOG = LoggerFactory.getLogger(CdcEventDeserializer.class);

        @Override
        public void deserialize(
                org.apache.kafka.connect.source.SourceRecord record, Collector<Event> out) {
            if (record == null) {
                return;
            }
            org.apache.kafka.connect.data.Struct valueStruct =
                    (org.apache.kafka.connect.data.Struct) record.value();
            if (valueStruct == null) {
                return;
            }

            String op = valueStruct.getString("op");
            if (op == null) {
                return;
            }

            org.apache.kafka.connect.data.Struct source = valueStruct.getStruct("source");
            if (source == null) {
                LOG.warn("Missing source info, skipping event");
                return;
            }
            String dbName = source.getString("db");
            String tableName = source.getString("table");

            org.apache.kafka.connect.data.Struct before = valueStruct.getStruct("before");
            org.apache.kafka.connect.data.Struct after = valueStruct.getStruct("after");

            Object[] beforeVals = extractValues(before);
            Object[] afterVals = extractValues(after);
            org.apache.flink.cdc.common.event.TableId tid =
                    org.apache.flink.cdc.common.event.TableId.tableId(dbName, tableName);

            switch (op) {
                case "c":
                case "r":
                    out.collect(DataChangeEvent.insertEvent(tid, GenericRecordData.of(afterVals)));
                    break;
                case "u":
                    out.collect(
                            DataChangeEvent.updateEvent(
                                    tid,
                                    GenericRecordData.of(beforeVals),
                                    GenericRecordData.of(afterVals)));
                    break;
                case "d":
                    out.collect(DataChangeEvent.deleteEvent(tid, GenericRecordData.of(beforeVals)));
                    break;
                case "t":
                    out.collect(new TruncateTableEvent(tid));
                    break;
                default:
                    LOG.debug("Unknown op: {}", op);
            }
        }

        private Object[] extractValues(org.apache.kafka.connect.data.Struct row) {
            if (row == null) {
                return null;
            }
            Object[] values = new Object[row.schema().fields().size()];
            int i = 0;
            for (org.apache.kafka.connect.data.Field f : row.schema().fields()) {
                values[i++] = convertValue(row.get(f.name()));
            }
            return values;
        }

        private Object convertValue(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof java.time.OffsetDateTime) {
                return java.sql.Timestamp.from(((java.time.OffsetDateTime) value).toInstant());
            }
            if (value instanceof java.time.ZonedDateTime) {
                return java.sql.Timestamp.from(((java.time.ZonedDateTime) value).toInstant());
            }
            if (value instanceof java.time.Instant) {
                return java.sql.Timestamp.from((java.time.Instant) value);
            }
            if (value instanceof java.time.LocalDateTime) {
                return java.sql.Timestamp.valueOf((java.time.LocalDateTime) value);
            }
            if (value instanceof java.util.Date) {
                return new java.sql.Timestamp(((java.util.Date) value).getTime());
            }
            if (value instanceof String) {
                String s = (String) value;
                if (s.contains("T")) {
                    try {
                        return java.sql.Timestamp.from(java.time.Instant.parse(s));
                    } catch (Exception e) {
                        return s;
                    }
                }
                return s;
            }
            return value;
        }

        @Override
        public TypeInformation<Event> getProducedType() {
            return TypeInformation.of(Event.class);
        }
    }

    @SuppressWarnings("deprecation")
    private static class CDBBatchSink extends RichSinkFunction<Event> {
        private static final Logger LOG = LoggerFactory.getLogger(CDBBatchSink.class);
        private final String sinkJdbcUrl;
        private final String sinkUser;
        private final String sinkPassword;
        private final int batchSize;
        private final long batchIntervalMs;
        private final Map<String, Map<String, String>> schemas;
        private final Map<String, String> pks;
        private transient Connection conn;
        private transient List<Record> batch;
        private transient long lastFlush;

        CDBBatchSink(
                String sinkJdbcUrl,
                String sinkUser,
                String sinkPassword,
                int batchSize,
                long batchIntervalMs,
                Map<String, Map<String, String>> schemas,
                Map<String, String> pks) {
            this.sinkJdbcUrl = sinkJdbcUrl;
            this.sinkUser = sinkUser;
            this.sinkPassword = sinkPassword;
            this.batchSize = batchSize;
            this.batchIntervalMs = batchIntervalMs;
            this.schemas = schemas;
            this.pks = pks;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(sinkJdbcUrl, sinkUser, sinkPassword);
            conn.setAutoCommit(false);
            batch = new ArrayList<>();
            lastFlush = System.currentTimeMillis();
            LOG.info("CDBBatchSink connected to sink");
        }

        @Override
        public void invoke(Event event, Context context) throws Exception {
            if (!(event instanceof DataChangeEvent)) {
                return;
            }
            batch.add(new Record((DataChangeEvent) event));
            if (batch.size() >= batchSize
                    || System.currentTimeMillis() - lastFlush >= batchIntervalMs) {
                flush();
            }
        }

        private void flush() throws Exception {
            if (batch.isEmpty()) {
                return;
            }
            List<Record> currentBatch = new ArrayList<>(batch);
            batch.clear();
            lastFlush = System.currentTimeMillis();

            Map<String, List<Record>> byTable = new LinkedHashMap<>();
            for (Record r : currentBatch) {
                byTable.computeIfAbsent(r.tableName, k -> new ArrayList<>()).add(r);
            }

            for (Map.Entry<String, List<Record>> e : byTable.entrySet()) {
                String table = e.getKey();
                Map<String, String> cols = schemas.get(table);
                if (cols == null || cols.isEmpty()) {
                    continue;
                }
                String fullTable = "`sink_" + table + "`";
                List<String> columnNames = new ArrayList<>(cols.keySet());

                List<Record> upserts = new ArrayList<>();
                List<Record> deletes = new ArrayList<>();
                for (Record r : e.getValue()) {
                    if (r.op == OperationType.DELETE) {
                        deletes.add(r);
                    } else {
                        upserts.add(r);
                    }
                }

                if (!upserts.isEmpty()) {
                    String colList = "`" + String.join("`,`", columnNames) + "`";
                    String placeholders =
                            String.join(",", Collections.nCopies(columnNames.size(), "?"));
                    String updateClause =
                            columnNames.stream()
                                    .map(c -> "`" + c + "`=?")
                                    .collect(Collectors.joining(","));
                    String sql =
                            String.format(
                                    "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                                    fullTable, colList, placeholders, updateClause);

                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        for (Record r : upserts) {
                            Object[] row = r.after;
                            if (row == null) {
                                continue;
                            }
                            for (int i = 0; i < columnNames.size(); i++) {
                                Object val = i < row.length ? row[i] : null;
                                ps.setObject(i + 1, val);
                                ps.setObject(i + 1 + columnNames.size(), val);
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    } catch (SQLException ex) {
                        LOG.error("Batch upsert failed for table {}: {}", table, ex.getMessage());
                        conn.rollback();
                        throw ex;
                    }
                }

                if (!deletes.isEmpty()) {
                    String pk = pks.getOrDefault(table, "id");
                    String deleteSql =
                            String.format("DELETE FROM %s WHERE `%s` = ?", fullTable, pk);

                    try (PreparedStatement ps = conn.prepareStatement(deleteSql)) {
                        for (Record r : deletes) {
                            Object[] row = r.before;
                            if (row == null) {
                                continue;
                            }
                            int pkIdx = columnNames.indexOf(pk);
                            if (pkIdx < 0) {
                                LOG.warn(
                                        "PK '{}' not found in columns for table {}, skipping delete",
                                        pk,
                                        table);
                                continue;
                            }
                            Object val = pkIdx < row.length ? row[pkIdx] : null;
                            ps.setObject(1, val);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    } catch (SQLException ex) {
                        LOG.error("Batch delete failed for table {}: {}", table, ex.getMessage());
                        conn.rollback();
                        throw ex;
                    }
                }
            }
            conn.commit();
        }

        @Override
        public void close() {
            try {
                if (batch != null && !batch.isEmpty()) {
                    flush();
                }
            } catch (Exception e) {
                LOG.error("Error during final flush", e);
            } finally {
                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    LOG.error("Error closing connection", e);
                }
                try {
                    super.close();
                } catch (Exception e) {
                    LOG.error("Error in super.close()", e);
                }
            }
        }

        private static class Record {
            final String tableName;
            final OperationType op;
            final Object[] before;
            final Object[] after;

            Record(DataChangeEvent event) {
                this.tableName = event.tableId().getTableName();
                this.op = event.op();
                this.before = extractValues(event.before());
                this.after = extractValues(event.after());
            }

            private static Object[] extractValues(org.apache.flink.cdc.common.data.RecordData row) {
                if (row == null) {
                    return null;
                }
                Object[] values = new Object[row.getArity()];
                for (int i = 0; i < row.getArity(); i++) {
                    values[i] = ((GenericRecordData) row).getField(i);
                }
                return values;
            }
        }
    }

    private static void createSinkTableIfNotExists(
            String jdbcUrl,
            String user,
            String pass,
            String table,
            Map<String, String> cols,
            String pk) {
        StringBuilder cl = new StringBuilder();
        int i = 0;
        for (Map.Entry<String, String> e : cols.entrySet()) {
            if (i > 0) {
                cl.append(", ");
            }
            cl.append("`").append(e.getKey()).append("` ").append(mapType(e.getValue()));
            i++;
        }
        String sql =
                String.format(
                        "CREATE TABLE IF NOT EXISTS `%s` (%s, PRIMARY KEY(`%s`))", table, cl, pk);
        try (Connection c = DriverManager.getConnection(jdbcUrl, user, pass);
                Statement s = c.createStatement()) {
            s.executeUpdate(sql);
            LOG.info("Sink table '{}' ready", table);
        } catch (SQLException e) {
            LOG.warn("Sink table create error (may already exist): {}", e.getMessage());
        }
    }

    static String mapType(String cdcType) {
        String upper = cdcType.toUpperCase();
        if (upper.contains("INT")) {
            return upper.contains("BIGINT") ? "BIGINT" : "INT";
        }
        if (upper.contains("VARCHAR") || upper.contains("CHAR")) {
            return cdcType;
        }
        if (upper.contains("TEXT")) {
            return "VARCHAR(1024)";
        }
        if (upper.contains("DECIMAL")) {
            return upper.matches(".*DECIMAL\\(\\d+,\\d+\\).*") ? cdcType : "DECIMAL(10,2)";
        }
        if (upper.contains("TIMESTAMP")) {
            return "TIMESTAMP(6)";
        }
        if (upper.contains("DATETIME")) {
            return "DATETIME(6)";
        }
        if (upper.contains("DATE")) {
            return "DATE";
        }
        if (upper.contains("TIME")) {
            return "TIME";
        }
        if (upper.contains("DOUBLE")) {
            return "DOUBLE";
        }
        if (upper.contains("FLOAT")) {
            return "FLOAT";
        }
        if (upper.contains("BOOLEAN")) {
            return "TINYINT(1)";
        }
        if (upper.contains("BLOB") || upper.contains("BINARY")) {
            return "BLOB";
        }
        LOG.warn("Unknown CDC type '{}', mapping to VARCHAR(1024)", cdcType);
        return "VARCHAR(1024)";
    }
}
