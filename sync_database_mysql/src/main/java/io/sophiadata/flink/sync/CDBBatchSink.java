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

    /** 初始化 JDBC 连接，关闭自动提交以支持批量事务。 */
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

    /**
     * 将攒积的批次写入数据库。流程： 1. 按表名分组 2. 每张表内按操作类型分为 upsert（insert/update）和 delete 3. upsert 使用 INSERT …
     * ON DUPLICATE KEY UPDATE（批量） 4. delete 使用 DELETE … WHERE pk = ?（批量） 5. 整批在一个事务中提交，失败时回滚
     */
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
                // 生成 INSERT INTO sink_xxx (col1,col2) VALUES (?,?) ON DUPLICATE KEY UPDATE
                // col1=?,col2=?
                // VALUES 和 UPDATE 绑定相同的值，MySQL 会根据主键自动判断是 insert 还是 update
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
                String deleteSql = String.format("DELETE FROM %s WHERE `%s` = ?", fullTable, pk);

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
    @SuppressWarnings("PMD.UseTryWithResources")
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

    @SuppressWarnings("PMD.ReturnEmptyCollectionRatherThanNull")
    static class Record {
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
