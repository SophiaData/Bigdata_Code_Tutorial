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

package io.sophiadata.flink.paimon.mongo;

import org.apache.flink.table.api.DataTypes;

import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MongoTypeMapperTest {

    @Test
    void testStringType() {
        assertEquals(DataTypes.STRING(), MongoTypeMapper.mapType(BsonType.STRING));
    }

    @Test
    void testInt32Type() {
        assertEquals(DataTypes.INT(), MongoTypeMapper.mapType(BsonType.INT32));
    }

    @Test
    void testInt64Type() {
        assertEquals(DataTypes.BIGINT(), MongoTypeMapper.mapType(BsonType.INT64));
    }

    @Test
    void testDoubleType() {
        assertEquals(DataTypes.DOUBLE(), MongoTypeMapper.mapType(BsonType.DOUBLE));
    }

    @Test
    void testBooleanType() {
        assertEquals(DataTypes.BOOLEAN(), MongoTypeMapper.mapType(BsonType.BOOLEAN));
    }

    @Test
    void testDateTimeType() {
        assertEquals(DataTypes.TIMESTAMP(3), MongoTypeMapper.mapType(BsonType.DATE_TIME));
    }

    @Test
    void testObjectIdType() {
        assertEquals(DataTypes.STRING(), MongoTypeMapper.mapType(BsonType.OBJECT_ID));
    }

    @Test
    void testArrayType() {
        assertEquals(DataTypes.STRING(), MongoTypeMapper.mapType(BsonType.ARRAY));
    }

    @Test
    void testDocumentType() {
        assertEquals(DataTypes.STRING(), MongoTypeMapper.mapType(BsonType.DOCUMENT));
    }

    @Test
    void testNullType() {
        assertEquals(DataTypes.NULL(), MongoTypeMapper.mapType(BsonType.NULL));
    }

    @Test
    void testDecimal128Type() {
        assertEquals(DataTypes.DECIMAL(38, 18), MongoTypeMapper.mapType(BsonType.DECIMAL128));
    }

    @Test
    void testBinaryType() {
        assertEquals(DataTypes.BYTES(), MongoTypeMapper.mapType(BsonType.BINARY));
    }

    @Test
    void testUnknownTypeDefaultsToString() {
        assertEquals(DataTypes.STRING(), MongoTypeMapper.mapType(BsonType.UNDEFINED));
    }
}
