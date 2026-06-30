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

import io.sophiadata.flink.sync.util.MysqlUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link MysqlUtil#mapType}. */
class FlinkSqlWDSTest {

    @Test
    void mapType_integerTypes() {
        assertEquals("BIGINT", MysqlUtil.mapType("BIGINT"));
        assertEquals("BIGINT", MysqlUtil.mapType("bigint"));
        assertEquals("INT", MysqlUtil.mapType("INT"));
        assertEquals("INT", MysqlUtil.mapType("INTEGER"));
        assertEquals("INT", MysqlUtil.mapType("TINYINT"));
        assertEquals("INT", MysqlUtil.mapType("SMALLINT"));
    }

    @Test
    void mapType_stringTypes() {
        assertEquals("VARCHAR(255)", MysqlUtil.mapType("VARCHAR(255)"));
        assertEquals("CHAR(10)", MysqlUtil.mapType("CHAR(10)"));
        assertEquals("VARCHAR(1024)", MysqlUtil.mapType("TEXT"));
        assertEquals("VARCHAR(1024)", MysqlUtil.mapType("LONGTEXT"));
    }

    @Test
    void mapType_decimalTypes() {
        assertEquals("DECIMAL(10,2)", MysqlUtil.mapType("DECIMAL(10,2)"));
        assertEquals("DECIMAL(10,2)", MysqlUtil.mapType("DECIMAL"));
        assertEquals("DECIMAL(10,2)", MysqlUtil.mapType("NUMERIC"));
    }

    @Test
    void mapType_dateTimeTypes() {
        assertEquals("TIMESTAMP(6)", MysqlUtil.mapType("TIMESTAMP"));
        assertEquals("TIMESTAMP(6)", MysqlUtil.mapType("TIMESTAMP(3)"));
        assertEquals("DATETIME(6)", MysqlUtil.mapType("DATETIME"));
        assertEquals("DATE", MysqlUtil.mapType("DATE"));
        assertEquals("TIME", MysqlUtil.mapType("TIME"));
    }

    @Test
    void mapType_floatingPointTypes() {
        assertEquals("DOUBLE", MysqlUtil.mapType("DOUBLE"));
        assertEquals("DOUBLE", MysqlUtil.mapType("DOUBLE PRECISION"));
        assertEquals("FLOAT", MysqlUtil.mapType("FLOAT"));
    }

    @Test
    void mapType_booleanType() {
        assertEquals("TINYINT(1)", MysqlUtil.mapType("BOOLEAN"));
        assertEquals("TINYINT(1)", MysqlUtil.mapType("BOOL"));
    }

    @Test
    void mapType_binaryTypes() {
        assertEquals("BLOB", MysqlUtil.mapType("BLOB"));
        assertEquals("BLOB", MysqlUtil.mapType("BINARY(16)"));
        assertEquals("BLOB", MysqlUtil.mapType("VARBINARY"));
    }

    @Test
    void mapType_unknownType_defaultsToVarchar() {
        assertEquals("VARCHAR(1024)", MysqlUtil.mapType("JSON"));
        assertEquals("VARCHAR(1024)", MysqlUtil.mapType("ENUM"));
        assertEquals("VARCHAR(1024)", MysqlUtil.mapType("UNKNOWN_TYPE"));
    }
}
