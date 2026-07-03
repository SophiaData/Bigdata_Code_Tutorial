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

package io.sophiadata.flink.sync.schema;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.AlterTableCommentEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import io.sophiadata.flink.sync.util.MysqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class SchemaEvolver implements java.io.Serializable, CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SchemaEvolver.class);

    /** State name for checkpoint. */
    private static final String STATE_NAME = "executed-alters";

    /**
     * Daemon-thread factory. Extracted as a named class so {@link SchemaEvolver} stays {@link
     * java.io.Serializable}: lambdas passed to {@link
     * java.util.concurrent.Executors#newCachedThreadPool} capture their enclosing context and are
     * not Serializable by default, which breaks Flink job submission when this object is captured
     * inside a {@code ProcessFunction}.
     */
    private static final class SchemaAlterThreadFactory
            implements ThreadFactory, java.io.Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        @SuppressWarnings("PMD.NullableProblems")
        public final Thread newThread(final Runnable r) {
            final Thread t = new Thread(r, "schema-alter");
            t.setDaemon(true);
            return t;
        }
    }

    /** Wrapper to make an ALTER statement checkpointable. */
    @SuppressWarnings("serial")
    private static final class AlterRecord implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        final String sqlStatement;
        final long checkpointTime;

        AlterRecord(final String sqlStatement, final long checkpointTime) {
            this.sqlStatement = sqlStatement;
            this.checkpointTime = checkpointTime;
        }

        @Override
        public String toString() {
            return sqlStatement;
        }
    }

    private final String sinkUrl;
    private final String sinkUser;
    private final String sinkPassword;
    private final String sinkTablePrefix;

    /**
     * {@code transient} because {@link java.util.concurrent.ThreadPoolExecutor} is technically
     * {@link java.io.Serializable} but contains a non-Serializable {@code handler} (default {@code
     * AbortPolicy}). We re-create it in {@link #readObject} after Flink deserializes the
     * SchemaEvolver on the TaskManager.
     */
    private transient ExecutorService alterExecutor;

    // --- CheckpointedFunction state ---
    /** In-memory set of already-executed ALTER statements (recovered from checkpoint). */
    private transient Set<String> executedAlters;

    /** Flink managed state descriptor. */
    private transient ListState<AlterRecord> alterState;

    /** Singleton JDBC connection for ALTER operations. Lazily initialized. */
    private transient volatile Connection alterConnection;

    public SchemaEvolver(final String sinkUrl, final String sinkUser, final String sinkPassword) {
        this(sinkUrl, sinkUser, sinkPassword, "");
    }

    public SchemaEvolver(
            final String sinkUrl,
            final String sinkUser,
            final String sinkPassword,
            final String sinkTablePrefix) {
        this.sinkUrl = sinkUrl;
        this.sinkUser = sinkUser;
        this.sinkPassword = sinkPassword;
        this.sinkTablePrefix = sinkTablePrefix;
        this.alterExecutor = Executors.newCachedThreadPool(new SchemaAlterThreadFactory());
        this.executedAlters = new HashSet<>();
    }

    private void readObject(final java.io.ObjectInputStream in)
            throws java.io.IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.alterExecutor = Executors.newCachedThreadPool(new SchemaAlterThreadFactory());
        this.executedAlters = new HashSet<>();
    }

    // ------------------------------------------------------------------------
    //  CheckpointedFunction
    // ------------------------------------------------------------------------

    @Override
    public void snapshotState(final FunctionSnapshotContext snapshotContext) throws Exception {
        alterState.clear();
        for (final String sqlStatement : executedAlters) {
            alterState.add(new AlterRecord(sqlStatement, snapshotContext.getCheckpointTimestamp()));
        }
    }

    @Override
    public void initializeState(final FunctionInitializationContext initContext) throws Exception {
        final ListStateDescriptor<AlterRecord> descriptor =
                new ListStateDescriptor<>(STATE_NAME, TypeInformation.of(new TypeHint<>() {}));

        alterState = initContext.getOperatorStateStore().getListState(descriptor);

        // Replay from checkpoint
        if (initContext.isRestored()) {
            executedAlters = new HashSet<>();
            int restoredCount = 0;
            for (final AlterRecord record : alterState.get()) {
                executedAlters.add(record.sqlStatement);
                restoredCount++;
            }
            LOG.info("SchemaEvolver restored {} executed alters from checkpoint", restoredCount);
        } else {
            executedAlters = new HashSet<>();
            LOG.info("SchemaEvolver initialized with empty alter history (fresh job)");
        }
    }

    public void processEvent(final Event event) {
        if (event instanceof SchemaChangeEvent) {
            final SchemaChangeEvent sce = (SchemaChangeEvent) event;
            dispatch(sce);
        }
    }

    private final <T, E extends Throwable> void dispatch(final SchemaChangeEvent event) throws E {
        SchemaChangeEventVisitor.visit(
                event,
                this::onAddColumn,
                this::onAlterColumnType,
                this::onCreateTable,
                this::onDropColumn,
                this::onDropTable,
                this::onRenameColumn,
                this::onTruncateTable,
                this::onAlterTableComment);
    }

    private final Void onCreateTable(final CreateTableEvent event) {
        final TableId tid = event.tableId();
        LOG.info("CreateTable: {}.{}", tid.getSchemaName(), tid.getTableName());
        return null;
    }

    private final Void onAddColumn(final AddColumnEvent event) {
        for (final AddColumnEvent.ColumnWithPosition addedColumn : event.getAddedColumns()) {
            final Column column = addedColumn.getAddColumn();
            final String columnName = column.getName();
            final String columnType = column.getType().toString();
            final String fullTable = fullTableName(event.tableId());
            LOG.info(
                    "AddColumn: {}.{} {} {}",
                    fullTable,
                    columnName,
                    columnType,
                    addedColumn.getPosition());
            alterExecutor.execute(() -> alterAddColumn(fullTable, columnName, columnType));
        }
        return null;
    }

    private final Void onDropColumn(final DropColumnEvent event) {
        for (final String droppedColumnName : event.getDroppedColumnNames()) {
            final String fullTable = fullTableName(event.tableId());
            LOG.info("DropColumn: {}.{}", fullTable, droppedColumnName);
            alterExecutor.execute(() -> alterDropColumn(fullTable, droppedColumnName));
        }
        return null;
    }

    private final Void onAlterColumnType(final AlterColumnTypeEvent event) {
        final Map<String, DataType> typeMapping = event.getTypeMapping();
        final String fullTable = fullTableName(event.tableId());
        for (final Map.Entry<String, DataType> typeEntry : typeMapping.entrySet()) {
            final String columnName = typeEntry.getKey();
            final String newColumnType = typeEntry.getValue().toString();
            LOG.info("AlterColumnType: {}.{} -> {}", fullTable, columnName, newColumnType);
            alterExecutor.execute(() -> alterColumnType(fullTable, columnName, newColumnType));
        }
        return null;
    }

    private final Void onRenameColumn(final RenameColumnEvent event) {
        final Map<String, String> nameMapping = event.getNameMapping();
        final String fullTable = fullTableName(event.tableId());
        for (final Map.Entry<String, String> nameEntry : nameMapping.entrySet()) {
            LOG.info(
                    "RenameColumn: {}.{} -> {}",
                    fullTable,
                    nameEntry.getKey(),
                    nameEntry.getValue());
            alterExecutor.execute(
                    () -> alterRenameColumn(fullTable, nameEntry.getKey(), nameEntry.getValue()));
        }
        return null;
    }

    private final Void onDropTable(final DropTableEvent event) {
        LOG.info("DropTable: {}", fullTableName(event.tableId()));
        return null;
    }

    private final Void onTruncateTable(final TruncateTableEvent event) {
        LOG.info("TruncateTable: {}", fullTableName(event.tableId()));
        return null;
    }

    @SuppressWarnings("PMD.UnusedFormalParameter")
    private final Void onAlterTableComment(final AlterTableCommentEvent event) {
        return null;
    }

    private final String fullTableName(final TableId tid) {
        final String table = tid.getTableName();
        return sinkTablePrefix + table;
    }

    private void alterAddColumn(
            final String fullTable, final String columnName, final String columnType) {
        final String sql =
                String.format(
                        "ALTER TABLE %s ADD COLUMN `%s` %s",
                        fullTable, columnName, mapToMysqlType(columnType));
        executeAlter(sql);
    }

    private void alterDropColumn(final String fullTable, final String columnName) {
        final String sql = String.format("ALTER TABLE %s DROP COLUMN `%s`", fullTable, columnName);
        executeAlter(sql);
    }

    private void alterColumnType(
            final String fullTable, final String columnName, final String newColumnType) {
        final String sql =
                String.format(
                        "ALTER TABLE %s MODIFY COLUMN `%s` %s",
                        fullTable, columnName, mapToMysqlType(newColumnType));
        executeAlter(sql);
    }

    private void alterRenameColumn(
            final String fullTable, final String oldColumnName, final String newColumnName) {
        final String sql =
                String.format(
                        "ALTER TABLE %s CHANGE COLUMN `%s` `%s`",
                        fullTable, oldColumnName, newColumnName);
        executeAlter(sql);
    }

    private void executeAlter(final String sqlStatement) {
        // Skip if already executed (idempotent on restart)
        if (executedAlters.contains(sqlStatement)) {
            LOG.debug("Already executed, skipping: {}", sqlStatement);
            return;
        }

        try (Statement statement = getAlterConnection().createStatement()) {
            LOG.info("Executing: {}", sqlStatement);
            statement.executeUpdate(sqlStatement);
            executedAlters.add(sqlStatement);
            LOG.info("ALTER executed successfully and recorded: {}", sqlStatement);
        } catch (SQLException e) {
            if (e.getErrorCode() == 1060
                    || (e.getMessage() != null && e.getMessage().contains("Duplicate column"))) {
                // Column already exists — add to history to prevent future checks
                executedAlters.add(sqlStatement);
                LOG.warn("Already exists, recorded as executed: {}", sqlStatement);
            } else {
                LOG.error("Failed: {}", sqlStatement, e);
            }
        }
    }

    /**
     * Gets or creates a singleton connection for ALTER statements. Uses double-checked locking for
     * thread safety.
     */
    private Connection getAlterConnection() throws SQLException {
        if (alterConnection == null || alterConnection.isClosed()) {
            synchronized (this) {
                if (alterConnection == null || alterConnection.isClosed()) {
                    alterConnection = DriverManager.getConnection(sinkUrl, sinkUser, sinkPassword);
                    LOG.debug("Created new ALTER connection for {}", sinkUrl);
                }
            }
        }
        return alterConnection;
    }

    private String mapToMysqlType(final String cdcType) {
        return MysqlUtil.mapType(cdcType);
    }

    public void shutdown() {
        try {
            if (alterConnection != null && !alterConnection.isClosed()) {
                alterConnection.close();
                LOG.info("ALTER connection closed");
            }
        } catch (SQLException e) {
            LOG.warn("Error closing ALTER connection: {}", e.getMessage());
        }
        alterExecutor.shutdown();
    }
}
