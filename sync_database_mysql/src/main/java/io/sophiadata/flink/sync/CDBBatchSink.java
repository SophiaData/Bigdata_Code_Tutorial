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

import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 批量 JDBC Sink —— 将 CDC 事件写入 MySQL sink 表。
 *
 * <p>核心设计：
 *
 * <ul>
 *   <li><b>批量写入</b>：攒够 batchSize 条或超过 batchIntervalMs 毫秒后统一 flush， 减少 JDBC 交互次数，提升吞吐。
 *   <li><b>Upsert 语义</b>：INSERT … ON DUPLICATE KEY UPDATE，同时处理新增和更新。
 *   <li><b>Delete 单独处理</b>：DELETE 只需主键，与 upsert 分开执行。
 *   <li><b>按表分组</b>：同一批次中的多条记录按表名分组，每张表生成独立的 SQL。
 * </ul>
 *
 * <p>schemas / pks 由外部（FlinkSqlWDS 的 process 函数）维护， 会在运行时随 DDL 变更动态更新，保证新建的表也能被正确写入。
 */
@SuppressWarnings("deprecation")
public class CDBBatchSink extends RichSinkFunction<Event> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CDBBatchSink.class);
    private final String sinkJdbcUrl;
    private final String sinkUser;
    private final String sinkPassword;
    private final int batchSize;
    private final long batchIntervalMs;
    /** Re-read schemas from the static holder to bypass Flink's serialized copy. */
    private Map<String, Map<String, String>> schemas() {
        return SharedSchemaState.schemas();
    }

    private Map<String, String> pks() {
        return SharedSchemaState.pks();
    }

    private transient Connection conn;
    private transient List<Record> batch;
    private transient long lastFlush;

    CDBBatchSink(
            final String sinkJdbcUrl,
            final String sinkUser,
            final String sinkPassword,
            final int batchSize,
            final long batchIntervalMs) {
        this.sinkJdbcUrl = sinkJdbcUrl;
        this.sinkUser = sinkUser;
        this.sinkPassword = sinkPassword;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
    }

    /** 初始化 JDBC 连接，关闭自动提交以支持批量事务。 */
    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.cj.jdbc.Driver");
        conn = DriverManager.getConnection(sinkJdbcUrl, sinkUser, sinkPassword);
        conn.setAutoCommit(false);
        // Verify connection is alive
        try (java.sql.Statement check = conn.createStatement();
                @SuppressWarnings("unused")
                        java.sql.ResultSet rs = check.executeQuery("SELECT 1")) {
            // Result consumed to verify the query executes without error
        }
        batch = new ArrayList<>();
        lastFlush = System.currentTimeMillis();
        LOG.info("CDBBatchSink connected to sink");
    }

    /** Reconnect if the current connection is broken. */
    private void ensureConnection() throws SQLException {
        if (conn == null || conn.isClosed() || !conn.isValid(5)) {
            LOG.warn("JDBC connection lost, reconnecting to sink");
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException ignored) {
            }
            conn = DriverManager.getConnection(sinkJdbcUrl, sinkUser, sinkPassword);
            conn.setAutoCommit(false);
            LOG.info("CDBBatchSink reconnected to sink");
        }
    }

    @Override
    public void invoke(final Event event, final Context context) throws Exception {
        if (!(event instanceof DataChangeEvent)) {
            return;
        }
        final Record rec = new Record((DataChangeEvent) event);
        batch.add(rec);
        // Flush whenever the batch fills up, when the timer interval has elapsed, or
        // when the batch already has pending rows and a new event arrives. The last
        // condition is important for snapshot-only loads where events arrive in a tight
        // burst and then stop — without it, the trailing 1-2 records sit in the batch
        // until the next event (which never comes) or until checkpoint close (when the
        // JDBC connection may already be torn down). The MySQL JDBC driver still batches
        // the resulting SQL internally, so per-event flushes are cheap.
        if (batch.size() >= batchSize
                || System.currentTimeMillis() - lastFlush >= batchIntervalMs
                || batch.size() > 0) {
            flush();
        }
    }

    /**
     * 将攒积的批次写入数据库。流程： 1. 按表名分组 2. 每张表内按操作类型分为 upsert（insert/update）和 delete 3. upsert 使用 INSERT …
     * ON DUPLICATE KEY UPDATE（批量） 4. delete 使用 DELETE … WHERE pk = ?（批量） 5. 整批在一个事务中提交，失败时回滚
     */
    private void flush() throws Exception {
        if (batch.isEmpty()) {
            return;
        }
        final List<Record> currentBatch = new ArrayList<>(batch);
        batch.clear();
        lastFlush = System.currentTimeMillis();

        try {
            flushInternal(currentBatch);
        } catch (SQLException ex) {
            LOG.warn("Flush failed, reconnecting and retrying once: {}", ex.getMessage());
            ensureConnection();
            flushInternal(currentBatch);
        }
    }

    private void flushInternal(final List<Record> currentBatch) throws Exception {
        final Map<String, List<Record>> byTable = groupByTable(currentBatch);

        for (final Map.Entry<String, List<Record>> e : byTable.entrySet()) {
            final String table = e.getKey();
            final Map<String, String> cols = schemas().get(table);
            if (cols == null || cols.isEmpty()) {
                LOG.warn(
                        "No schema for table '{}' — skipping {} records. schemas keys: {}",
                        table,
                        e.getValue().size(),
                        schemas().keySet());
                continue;
            }
            LOG.info("Flush table '{}': columns={}", table, cols.keySet());
            final String fullTable = "`sink_" + table + "`";
            final List<String> columnNames = new ArrayList<>(cols.keySet());

            final List<Record> upserts = new ArrayList<>();
            final List<Record> deletes = new ArrayList<>();
            splitByOperation(e.getValue(), upserts, deletes);

            flushUpserts(table, fullTable, columnNames, upserts);
            flushDeletes(table, fullTable, columnNames, deletes);
        }
        conn.commit();
    }

    private final Map<String, List<Record>> groupByTable(final List<Record> records) {
        final Map<String, List<Record>> byTable = new LinkedHashMap<>();
        for (final Record r : records) {
            byTable.computeIfAbsent(r.tableName, k -> new ArrayList<>()).add(r);
        }
        return byTable;
    }

    private void splitByOperation(
            final List<Record> records, final List<Record> upserts, final List<Record> deletes) {
        for (final Record r : records) {
            if (r.op == OperationType.DELETE) {
                deletes.add(r);
            } else {
                upserts.add(r);
            }
        }
    }

    private void flushUpserts(
            final String table,
            final String fullTable,
            final List<String> columnNames,
            final List<Record> upserts)
            throws SQLException {
        if (upserts.isEmpty()) {
            return;
        }
        ensureConnection();
        final String colList = "`" + String.join("`,`", columnNames) + "`";
        final String placeholders = String.join(",", Collections.nCopies(columnNames.size(), "?"));
        final String updateClause =
                columnNames.stream().map(c -> "`" + c + "`=?").collect(Collectors.joining(","));
        final String sql =
                String.format(
                        "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                        fullTable, colList, placeholders, updateClause);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (final Record r : upserts) {
                if (r.after == null) {
                    continue;
                }
                bindRow(ps, r.after, columnNames.size());
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException ex) {
            LOG.error("Batch upsert failed for table {}: {}", table, ex.getMessage());
            conn.rollback();
            throw ex;
        }
    }

    private void flushDeletes(
            final String table,
            final String fullTable,
            final List<String> columnNames,
            final List<Record> deletes)
            throws SQLException {
        if (deletes.isEmpty()) {
            return;
        }
        ensureConnection();
        final String pk = pks().getOrDefault(table, "id");
        final String deleteSql = String.format("DELETE FROM %s WHERE `%s` = ?", fullTable, pk);

        try (PreparedStatement ps = conn.prepareStatement(deleteSql)) {
            for (final Record r : deletes) {
                if (r.before == null) {
                    continue;
                }
                final int pkIdx = columnNames.indexOf(pk);
                if (pkIdx < 0) {
                    LOG.warn(
                            "PK '{}' not found in columns for table {}, skipping delete",
                            pk,
                            table);
                    continue;
                }
                ps.setObject(1, pkIdx < r.before.length ? r.before[pkIdx] : null);
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException ex) {
            LOG.error("Batch delete failed for table {}: {}", table, ex.getMessage());
            conn.rollback();
            throw ex;
        }
    }

    private void bindRow(final PreparedStatement ps, final Object[] row, final int columnCount)
            throws SQLException {
        for (int i = 0; i < columnCount; i++) {
            final Object val = i < row.length ? row[i] : null;
            ps.setObject(i + 1, val);
            ps.setObject(i + 1 + columnCount, val);
        }
    }

    @Override
    @SuppressWarnings("PMD.UseTryWithResources")
    public void close() {
        try {
            if (batch != null && !batch.isEmpty()) {
                try {
                    flush();
                } catch (Exception e) {
                    LOG.warn("Final flush failed, attempting reconnect: {}", e.getMessage());
                    try {
                        ensureConnection();
                        flush();
                    } catch (Exception e2) {
                        LOG.error("Final flush failed after reconnect", e2);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Error during final flush", e);
        } finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                LOG.debug("Error closing connection (may already be closed)", e.getMessage());
            }
            try {
                super.close();
            } catch (Exception e) {
                LOG.debug("Error in super.close(): {}", e.getMessage());
            }
        }
    }

    @SuppressWarnings("PMD.ReturnEmptyCollectionRatherThanNull")
    static class Record {
        final String tableName;
        final OperationType op;
        final Object[] before;
        final Object[] after;

        Record(final DataChangeEvent event) {
            this.tableName = event.tableId().getTableName();
            this.op = event.op();
            this.before = extractValues(event.before());
            this.after = extractValues(event.after());
        }

        private static Object[] extractValues(
                final org.apache.flink.cdc.common.data.RecordData row) {
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
