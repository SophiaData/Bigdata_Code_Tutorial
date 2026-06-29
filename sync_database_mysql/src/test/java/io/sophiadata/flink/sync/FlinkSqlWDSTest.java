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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link FlinkSqlWDS}. */
class FlinkSqlWDSTest {

    @Test
    void mapType_integerTypes() {
        assertEquals("BIGINT", FlinkSqlWDS.mapType("BIGINT"));
        assertEquals("BIGINT", FlinkSqlWDS.mapType("bigint"));
        assertEquals("INT", FlinkSqlWDS.mapType("INT"));
        assertEquals("INT", FlinkSqlWDS.mapType("INTEGER"));
        assertEquals("INT", FlinkSqlWDS.mapType("TINYINT"));
        assertEquals("INT", FlinkSqlWDS.mapType("SMALLINT"));
    }

    @Test
    void mapType_stringTypes() {
        assertEquals("VARCHAR(255)", FlinkSqlWDS.mapType("VARCHAR(255)"));
        assertEquals("CHAR(10)", FlinkSqlWDS.mapType("CHAR(10)"));
        assertEquals("VARCHAR(1024)", FlinkSqlWDS.mapType("TEXT"));
        assertEquals("VARCHAR(1024)", FlinkSqlWDS.mapType("LONGTEXT"));
    }

    @Test
    void mapType_decimalTypes() {
        assertEquals("DECIMAL(10,2)", FlinkSqlWDS.mapType("DECIMAL(10,2)"));
        assertEquals("DECIMAL(10,2)", FlinkSqlWDS.mapType("DECIMAL"));
        assertEquals("DECIMAL(10,2)", FlinkSqlWDS.mapType("NUMERIC"));
    }

    @Test
    void mapType_dateTimeTypes() {
        assertEquals("TIMESTAMP(6)", FlinkSqlWDS.mapType("TIMESTAMP"));
        assertEquals("TIMESTAMP(6)", FlinkSqlWDS.mapType("TIMESTAMP(3)"));
        assertEquals("DATETIME(6)", FlinkSqlWDS.mapType("DATETIME"));
        assertEquals("DATE", FlinkSqlWDS.mapType("DATE"));
        assertEquals("TIME", FlinkSqlWDS.mapType("TIME"));
    }

    @Test
    void mapType_floatingPointTypes() {
        assertEquals("DOUBLE", FlinkSqlWDS.mapType("DOUBLE"));
        assertEquals("DOUBLE", FlinkSqlWDS.mapType("DOUBLE PRECISION"));
        assertEquals("FLOAT", FlinkSqlWDS.mapType("FLOAT"));
    }

    @Test
    void mapType_booleanType() {
        assertEquals("TINYINT(1)", FlinkSqlWDS.mapType("BOOLEAN"));
        assertEquals("TINYINT(1)", FlinkSqlWDS.mapType("BOOL"));
    }

    @Test
    void mapType_binaryTypes() {
        assertEquals("BLOB", FlinkSqlWDS.mapType("BLOB"));
        assertEquals("BLOB", FlinkSqlWDS.mapType("BINARY(16)"));
        assertEquals("BLOB", FlinkSqlWDS.mapType("VARBINARY"));
    }

    @Test
    void mapType_unknownType_defaultsToVarchar() {
        assertEquals("VARCHAR(1024)", FlinkSqlWDS.mapType("JSON"));
        assertEquals("VARCHAR(1024)", FlinkSqlWDS.mapType("ENUM"));
        assertEquals("VARCHAR(1024)", FlinkSqlWDS.mapType("UNKNOWN_TYPE"));
    }
}
