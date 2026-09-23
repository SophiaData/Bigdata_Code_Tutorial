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

package io.sophiadata.flink.cdc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import io.sophiadata.flink.sync.table.CustomDebeziumDeserializer;
import io.sophiadata.flink.sync.util.MysqlTypeMapper;
import io.sophiadata.flink.sync.util.ParameterUtil;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Version-agnostic behaviour of the MySQL sync layer.
 *
 * <p>These run without Docker or a database, so they execute on every build and on both supported
 * Flink lines (1.20 and 2.2). They are the regression net for the parts of the code that previously
 * only failed at runtime.
 *
 * <p>(@sophiadata) (@date 2023/6/1 14:23).
 */
class CdcCompatTest {

    @Test
    void tableListIsNormalisedToTheConnectorFormat() {
        assertEquals("test.*", ParameterUtil.normalizeTableList("test", ".*"));
        assertEquals("test.*", ParameterUtil.normalizeTableList("test", null));
        assertEquals("test.*", ParameterUtil.normalizeTableList("test", "  "));
        assertEquals("test.orders", ParameterUtil.normalizeTableList("test", "orders"));
        assertEquals(
                "test.orders,test.users", ParameterUtil.normalizeTableList("test", "orders,users"));
        // Already-qualified names pass through unchanged.
        assertEquals("shop.orders", ParameterUtil.normalizeTableList("test", "shop.orders"));
        // Blank entries are dropped rather than producing a malformed pattern.
        assertEquals("test.orders", ParameterUtil.normalizeTableList("test", "orders,"));
    }

    @Test
    void deserializerReportsTheTupleTypeItProduces() {
        Map<String, RowType> tableRowTypeMap = new HashMap<>();
        tableRowTypeMap.put(
                "orders",
                (RowType)
                        org.apache.flink.table.api.DataTypes.ROW(
                                        org.apache.flink.table.api.DataTypes.FIELD(
                                                "id", org.apache.flink.table.api.DataTypes.INT()),
                                        org.apache.flink.table.api.DataTypes.FIELD(
                                                "name",
                                                org.apache.flink.table.api.DataTypes.STRING()))
                                .getLogicalType());

        CustomDebeziumDeserializer deserializer = new CustomDebeziumDeserializer(tableRowTypeMap);
        TypeInformation<Tuple2<String, Row>> produced = deserializer.getProducedType();

        assertNotNull(produced);
        assertEquals(Tuple2.class, produced.getTypeClass());
    }

    /**
     * The deserializer must be serializable: Flink serializes the source before shipping it to task
     * managers, and a non-serializable deserializer fails at submission with {@code
     * StreamTaskException: Could not serialize object for key serializedUDF}.
     *
     * <p>This is a cheap guard for a defect that only a real job would otherwise catch.
     */
    @Test
    void deserializerIsSerializable() {
        Map<String, RowType> tableRowTypeMap = new HashMap<>();
        tableRowTypeMap.put(
                "orders",
                (RowType)
                        org.apache.flink.table.api.DataTypes.ROW(
                                        org.apache.flink.table.api.DataTypes.FIELD(
                                                "id", org.apache.flink.table.api.DataTypes.INT()))
                                .getLogicalType());

        CustomDebeziumDeserializer deserializer = new CustomDebeziumDeserializer(tableRowTypeMap);

        assertTrue(
                deserializer instanceof java.io.Serializable,
                "deserializer must implement Serializable");

        // Round-trip it to prove every field really is serializable, not just the declared type.
        try (java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
                java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(bytes)) {
            out.writeObject(deserializer);
            out.flush();
            assertTrue(bytes.size() > 0, "serialized form should not be empty");

            try (java.io.ObjectInputStream in =
                    new java.io.ObjectInputStream(
                            new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
                Object restored = in.readObject();
                assertTrue(restored instanceof CustomDebeziumDeserializer);
            }
        } catch (Exception e) {
            throw new AssertionError("deserializer is not serializable", e);
        }
    }

    @Test
    void typeMapperHandlesTheMysqlTypesThatMatter() {
        MysqlTypeMapper mapper = MysqlTypeMapper.standard();

        // TINYINT(1) is MySQL's boolean idiom.
        assertEquals(
                "BOOLEAN",
                mapper.toFlinkType("flag", Types.TINYINT, "TINYINT", 1, 0, true)
                        .getLogicalType()
                        .asSummaryString());
        // A genuine TINYINT keeps its numeric type.
        assertEquals(
                "TINYINT",
                mapper.toFlinkType("tiny", Types.TINYINT, "TINYINT", 4, 0, true)
                        .getLogicalType()
                        .asSummaryString());
        // Unsigned types widen so the value range fits.
        assertEquals(
                "SMALLINT",
                mapper.toFlinkType("u", Types.TINYINT, "TINYINT UNSIGNED", 3, 0, true)
                        .getLogicalType()
                        .asSummaryString());
        assertEquals(
                "BIGINT",
                mapper.toFlinkType("u", Types.INTEGER, "INT UNSIGNED", 10, 0, true)
                        .getLogicalType()
                        .asSummaryString());
        assertEquals(
                "DECIMAL(20, 0)",
                mapper.toFlinkType("u", Types.BIGINT, "BIGINT UNSIGNED", 20, 0, true)
                        .getLogicalType()
                        .asSummaryString());
        // MySQL TIMESTAMP and DATETIME both map to a TIMESTAMP family type.
        assertTrue(
                mapper.toFlinkType("ts", Types.TIMESTAMP, "TIMESTAMP", 19, 0, true)
                        .getLogicalType()
                        .asSummaryString()
                        .contains("TIMESTAMP"));
        assertTrue(
                mapper.toFlinkType("dt", Types.TIMESTAMP, "DATETIME", 19, 0, true)
                        .getLogicalType()
                        .asSummaryString()
                        .contains("TIMESTAMP"));
        // Decimal precision is preserved rather than flattened.
        assertEquals(
                "DECIMAL(10, 2)",
                mapper.toFlinkType("amount", Types.DECIMAL, "DECIMAL", 10, 2, true)
                        .getLogicalType()
                        .asSummaryString());
        // JSON arrives as Types.OTHER and degrades to STRING.
        assertEquals(
                "STRING",
                mapper.toFlinkType("doc", Types.OTHER, "JSON", 0, 0, true)
                        .getLogicalType()
                        .asSummaryString());
    }

    @Test
    void typeMapperCarriesNullabilityOntoTheFlinkType() {
        MysqlTypeMapper mapper = MysqlTypeMapper.standard();
        assertFalse(
                mapper.toFlinkType("c", Types.INTEGER, "INT", 10, 0, false)
                        .getLogicalType()
                        .isNullable());
        assertTrue(
                mapper.toFlinkType("c", Types.INTEGER, "INT", 10, 0, true)
                        .getLogicalType()
                        .isNullable());
    }

    @Test
    void sinkTableDdlDoesNotSplitTypesContainingCommas() {
        // Regression: an earlier implementation split the generated DDL on "," to inject timestamp
        // defaults, which corrupted DECIMAL(10,2) into two fragments.
        String ddl =
                io.sophiadata.flink.sync.util.MysqlUtil.createTable(
                        "sink_orders",
                        new String[] {"id", "amount", "created_at"},
                        new org.apache.flink.table.types.DataType[] {
                            org.apache.flink.table.api.DataTypes.BIGINT().notNull(),
                            org.apache.flink.table.api.DataTypes.DECIMAL(10, 2),
                            org.apache.flink.table.api.DataTypes.TIMESTAMP(0)
                        },
                        Arrays.asList("id"));

        assertTrue(ddl.contains("DECIMAL(10, 2)"), "decimal precision must survive intact: " + ddl);
        assertTrue(ddl.contains("`id` BIGINT"), ddl);
        assertTrue(ddl.contains("PRIMARY KEY (`id`)"), ddl);
        // MySQL TIMESTAMP columns need an explicit default.
        assertTrue(ddl.contains("default '1970-01-01 09:00:00'"), ddl);
    }

    @Test
    void sinkTableDdlRequiresAPrimaryKey() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                io.sophiadata.flink.sync.util.MysqlUtil.createTable(
                                        "sink_x",
                                        new String[] {"id"},
                                        new org.apache.flink.table.types.DataType[] {
                                            org.apache.flink.table.api.DataTypes.INT()
                                        },
                                        java.util.Collections.emptyList()));
        assertTrue(ex.getMessage().contains("primary key"), ex.getMessage());
    }
}
