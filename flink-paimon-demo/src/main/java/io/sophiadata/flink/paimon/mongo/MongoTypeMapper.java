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
import org.apache.flink.table.types.DataType;

import org.bson.BsonType;

/** Maps MongoDB BsonType to Flink DataType for Paimon schema generation. */
public final class MongoTypeMapper {
    private MongoTypeMapper() {}

    public static DataType mapType(final BsonType bsonType) {
        switch (bsonType) {
            case STRING:
                return DataTypes.STRING();
            case INT32:
                return DataTypes.INT();
            case INT64:
                return DataTypes.BIGINT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case DATE_TIME:
                return DataTypes.TIMESTAMP(3);
            case OBJECT_ID:
                return DataTypes.STRING();
            case ARRAY:
                return DataTypes.STRING();
            case DOCUMENT:
                return DataTypes.STRING();
            case NULL:
                return DataTypes.NULL();
            case DECIMAL128:
                return DataTypes.DECIMAL(38, 18);
            case BINARY:
                return DataTypes.BYTES();
            default:
                return DataTypes.STRING();
        }
    }
}
