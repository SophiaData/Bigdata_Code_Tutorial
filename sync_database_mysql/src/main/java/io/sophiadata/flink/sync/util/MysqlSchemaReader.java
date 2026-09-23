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

package io.sophiadata.flink.sync.util;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads MySQL table metadata over plain JDBC.
 *
 * <p>This replaces the previous use of Flink's {@code MySqlCatalog} for schema introspection. That
 * class was an internal part of {@code flink-connector-jdbc} whose package moved between Flink
 * releases (FLIP-449 reorganized the connector into per-database artifacts in 1.18+, relocating
 * {@code MySqlCatalog} from {@code ...jdbc.databases.mysql.catalog} to {@code
 * ...jdbc.mysql.database.catalog}), so depending on it pinned this project to a single Flink minor.
 * {@link DatabaseMetaData} has been stable for decades and works on every version.
 */
public final class MysqlSchemaReader {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlSchemaReader.class);

    private MysqlSchemaReader() {}

    /** Column metadata for one table. */
    public static final class TableSchema {

        private final String tableName;
        private final String[] fieldNames;
        private final DataType[] fieldDataTypes;
        private final RowType rowType;
        private final List<String> primaryKeys;

        TableSchema(
                String tableName,
                String[] fieldNames,
                DataType[] fieldDataTypes,
                RowType rowType,
                List<String> primaryKeys) {
            this.tableName = tableName;
            this.fieldNames = fieldNames;
            this.fieldDataTypes = fieldDataTypes;
            this.rowType = rowType;
            this.primaryKeys = primaryKeys;
        }

        public String tableName() {
            return tableName;
        }

        public String[] fieldNames() {
            return fieldNames;
        }

        public DataType[] fieldDataTypes() {
            return fieldDataTypes;
        }

        public RowType rowType() {
            return rowType;
        }

        public List<String> primaryKeys() {
            return primaryKeys;
        }
    }

    /**
     * Lists the base tables of a database.
     *
     * @param connection an open connection
     * @param databaseName the schema/database name
     * @return table names in discovery order
     * @throws SQLException if metadata cannot be read
     */
    public static List<String> listTables(Connection connection, String databaseName)
            throws SQLException {
        List<String> tables = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getTables(databaseName, null, "%", new String[] {"TABLE"})) {
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
        }
        return tables;
    }

    /**
     * Reads the column layout and primary key of a table.
     *
     * @param connection an open connection
     * @param databaseName the schema/database name
     * @param tableName the table name
     * @param typeMapper converts JDBC types to Flink {@link DataType}s
     * @return the table schema
     * @throws SQLException if metadata cannot be read
     */
    public static TableSchema readTableSchema(
            Connection connection, String databaseName, String tableName, JdbcTypeMapper typeMapper)
            throws SQLException {

        DatabaseMetaData metaData = connection.getMetaData();

        List<String> names = new ArrayList<>();
        List<DataType> dataTypes = new ArrayList<>();
        List<LogicalType> logicalTypes = new ArrayList<>();

        try (ResultSet rs = metaData.getColumns(databaseName, null, tableName, "%")) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                int jdbcType = rs.getInt("DATA_TYPE");
                String typeName = rs.getString("TYPE_NAME");
                int size = rs.getInt("COLUMN_SIZE");
                int scale = rs.getInt("DECIMAL_DIGITS");
                boolean nullable = rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls;

                DataType dataType =
                        typeMapper.toFlinkType(
                                columnName, jdbcType, typeName, size, scale, nullable);
                names.add(columnName);
                dataTypes.add(dataType);
                logicalTypes.add(dataType.getLogicalType());
            }
        }

        if (names.isEmpty()) {
            throw new SQLException(
                    "Table '" + databaseName + "." + tableName + "' has no readable columns");
        }

        List<String> primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(databaseName, null, tableName)) {
            // getPrimaryKeys is not ordered; KEY_SEQ gives the composite key order.
            Map<Short, String> ordered = new LinkedHashMap<>();
            while (rs.next()) {
                ordered.put(rs.getShort("KEY_SEQ"), rs.getString("COLUMN_NAME"));
            }
            primaryKeys.addAll(ordered.values());
        }

        String[] fieldNames = names.toArray(new String[0]);
        DataType[] fieldDataTypes = dataTypes.toArray(new DataType[0]);
        RowType rowType = RowType.of(logicalTypes.toArray(new LogicalType[0]), fieldNames);

        LOG.debug(
                "Read schema for {}.{}: {} columns, primary key {}",
                databaseName,
                tableName,
                fieldNames.length,
                primaryKeys);

        return new TableSchema(tableName, fieldNames, fieldDataTypes, rowType, primaryKeys);
    }

    /** Converts JDBC column metadata into a Flink {@link DataType}. */
    public interface JdbcTypeMapper {

        /**
         * Maps one JDBC column to a Flink type.
         *
         * @param columnName the column name
         * @param jdbcType the {@link java.sql.Types} constant
         * @param typeName the database-specific type name
         * @param size the column size
         * @param scale the decimal scale
         * @param nullable whether the column allows nulls
         * @return the Flink data type
         */
        DataType toFlinkType(
                String columnName,
                int jdbcType,
                String typeName,
                int size,
                int scale,
                boolean nullable);
    }
}
