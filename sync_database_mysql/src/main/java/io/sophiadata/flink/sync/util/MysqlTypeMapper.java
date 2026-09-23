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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.sql.Types;

/**
 * Maps MySQL/JDBC column types to Flink {@link DataType}s.
 *
 * <p>Replaces the type information that previously came from Flink's internal {@code MySqlCatalog},
 * making the schema reader independent of the Flink minor version.
 *
 * <p>Type mapping notes:
 *
 * <ul>
 *   <li>{@code TINYINT(1)} is treated as {@code BOOLEAN}, matching the MySQL convention and
 *       Debezium behaviour.
 *   <li>Unsigned integer types are widened because their value range exceeds the signed Flink type
 *       (e.g. {@code TINYINT UNSIGNED} becomes {@code SMALLINT}).
 *   <li>{@code DATETIME} maps to {@code TIMESTAMP(0)} and {@code TIMESTAMP} to {@code
 *       TIMESTAMP_LTZ}; MySQL {@code TIMESTAMP} is stored in UTC and converted by session time
 *       zone.
 * </ul>
 */
public final class MysqlTypeMapper implements MysqlSchemaReader.JdbcTypeMapper {

    /** Upper bound Flink uses when a MySQL type has no explicit length. */
    private static final int DEFAULT_VARCHAR_LENGTH = 65535;

    @Override
    public DataType toFlinkType(
            String columnName,
            int jdbcType,
            String typeName,
            int size,
            int scale,
            boolean nullable) {

        String normalized = typeName == null ? "" : typeName.toUpperCase();
        boolean unsigned = normalized.contains("UNSIGNED");

        DataType type;
        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                type = DataTypes.BOOLEAN();
                break;

            case Types.TINYINT:
                // MySQL reports TINYINT(1) as BIT/BOOLEAN; a genuine tinyint keeps its type.
                if (size == 1 && !unsigned) {
                    type = DataTypes.BOOLEAN();
                } else {
                    type = unsigned ? DataTypes.SMALLINT() : DataTypes.TINYINT();
                }
                break;

            case Types.SMALLINT:
                type = unsigned ? DataTypes.INT() : DataTypes.SMALLINT();
                break;

            case Types.INTEGER:
                type = unsigned ? DataTypes.BIGINT() : DataTypes.INT();
                break;

            case Types.BIGINT:
                // BIGINT UNSIGNED exceeds BIGINT; DECIMAL(20,0) is the lossless representation.
                type = unsigned ? DataTypes.DECIMAL(20, 0) : DataTypes.BIGINT();
                break;

            case Types.REAL:
            case Types.FLOAT:
                type = DataTypes.FLOAT();
                break;

            case Types.DOUBLE:
                type = DataTypes.DOUBLE();
                break;

            case Types.NUMERIC:
            case Types.DECIMAL:
                int precision = size > 0 ? Math.min(size, 65) : 10;
                int decimalScale = Math.max(scale, 0);
                type = DataTypes.DECIMAL(precision, decimalScale);
                break;

            case Types.CHAR:
                type = DataTypes.CHAR(size > 0 ? size : 1);
                break;

            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                type = DataTypes.STRING();
                break;

            case Types.DATE:
                type = DataTypes.DATE();
                break;

            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                type = DataTypes.TIME();
                break;

            case Types.TIMESTAMP:
                // MySQL TIMESTAMP is time-zone aware; DATETIME is not.
                type =
                        "TIMESTAMP".equals(normalized)
                                ? DataTypes.TIMESTAMP_LTZ(0)
                                : DataTypes.TIMESTAMP(0);
                break;

            case Types.TIMESTAMP_WITH_TIMEZONE:
                type = DataTypes.TIMESTAMP_LTZ(0);
                break;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
            case Types.CLOB:
            case Types.NCLOB:
                type = DataTypes.BYTES();
                break;

            case Types.OTHER:
            case Types.JAVA_OBJECT:
                // MySQL JSON and spatial types arrive as OTHER.
                if (normalized.contains("JSON")) {
                    type = DataTypes.STRING();
                } else if (normalized.contains("GEOMETRY")
                        || normalized.contains("POINT")
                        || normalized.contains("LINESTRING")
                        || normalized.contains("POLYGON")) {
                    type = DataTypes.BYTES();
                } else if (normalized.contains("ENUM") || normalized.contains("SET")) {
                    type = DataTypes.STRING();
                } else {
                    type = DataTypes.STRING();
                }
                break;

            case Types.ARRAY:
                type = DataTypes.ARRAY(DataTypes.STRING());
                break;

            default:
                // Unknown types degrade to STRING rather than failing the whole sync; the table
                // DDL printed at startup makes the substitution visible.
                type = DataTypes.STRING();
                break;
        }

        return nullable ? type.nullable() : type.notNull();
    }

    /**
     * Convenience factory for the default MySQL mapping.
     *
     * @return a stateless mapper
     */
    public static MysqlTypeMapper standard() {
        return new MysqlTypeMapper();
    }

    /**
     * Renders a Flink {@link DataType} as a MySQL column type for the sink DDL.
     *
     * <p>Kept alongside the reverse mapping so the two stay obviously consistent.
     *
     * @param type the Flink type
     * @return the MySQL column definition
     */
    public static String toMysqlType(DataType type) {
        return type.getLogicalType().asSerializableString();
    }
}
