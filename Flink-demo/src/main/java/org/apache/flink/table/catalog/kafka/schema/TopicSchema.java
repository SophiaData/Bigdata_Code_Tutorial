/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.kafka.schema;

import org.apache.flink.table.api.Schema;
import org.apache.flink.util.Preconditions;

/** (@SophiaData) (@date 2023/3/6 09:53). */
public class TopicSchema {
    private final Schema schema;
    private final SchemaType schemaType;

    public TopicSchema(Schema schema, String schemaType) {
        this(schema, Enum.valueOf(SchemaType.class, schemaType));
    }

    public TopicSchema(Schema schema, SchemaType schemaType) {
        Preconditions.checkNotNull(schemaType);
        this.schema = schema;
        this.schemaType = schemaType;
    }

    public Schema getSchema() {
        return schema;
    }

    public SchemaType getSchemaType() {
        return schemaType;
    }

    /** */
    public enum SchemaType {
        AVRO("avro", "avro-confluent"),
        JSON("json", "json"),
        RAW("raw", "raw");

        private final String value;
        private final String format;

        SchemaType(final String value, final String format) {
            this.value = value;
            this.format = format;
        }

        public String getValue() {
            return value;
        }

        public String getFormat() {
            return format;
        }

        @Override
        public String toString() {
            return this.getValue();
        }
    }
}
