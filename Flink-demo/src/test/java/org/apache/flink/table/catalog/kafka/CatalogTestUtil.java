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

package org.apache.flink.table.catalog.kafka;

/** (@SophiaData) (@date 2023/3/6 10:23). */
import org.apache.flink.table.catalog.ObjectPath;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.Collections;
import java.util.List;

/** */
public class CatalogTestUtil {
    public static final List<String> SCHEMA_REGISTRY_URIS = Collections.singletonList("mock://");
    public static final String CATALOG_NAME = "kafka";

    public static final String TABLE1 = "t1";
    public static final ObjectPath TABLE1PATH = new ObjectPath(KafkaCatalog.DEFAULT_DB, TABLE1);
    private static final Schema table1Schema =
            SchemaBuilder.record(TABLE1)
                    .fields()
                    .name("name")
                    .type(Schema.create(Schema.Type.STRING))
                    .noDefault()
                    .name("age")
                    .type(Schema.create(Schema.Type.INT))
                    .noDefault()
                    .endRecord();
    public static final AvroSchema TABLE1AVROSCHEMA = new AvroSchema(table1Schema);
    public static final GenericRecord TABLE1MESSAGE =
            new GenericRecordBuilder(table1Schema).set("name", "Abcd").set("age", 30).build();

    public static final String TABLE2 = "t2";
    public static final String TABLE2_TARGET = "table2_target";
    public static final ObjectPath TABLE2PATH = new ObjectPath(KafkaCatalog.DEFAULT_DB, TABLE2);
    public static final String TABLE2IDENTIFIER =
            "`" + CATALOG_NAME + "`.`" + KafkaCatalog.DEFAULT_DB + "`." + TABLE2;
    public static final String TABLE2_TARGETIDENTIFIER =
            "`" + CATALOG_NAME + "`.`" + KafkaCatalog.DEFAULT_DB + "`." + TABLE2_TARGET;
    public static final JsonSchema TABLE2JSONSCHEMA =
            new JsonSchema(
                    "{\n"
                            + "  \"title\": \""
                            + TABLE2
                            + "\",\n"
                            + "  \"type\": \"object\",\n"
                            + "  \"required\": [\"name\",\"age\"],  \n"
                            + "  \"properties\": {\n"
                            + "    \"name\": {\n"
                            + "      \"type\": [\"string\",\"null\"],\n"
                            + "      \"description\": \"The name.\"\n"
                            + "    },\n"
                            + "    \"age\": {\n"
                            + "      \"description\": \"Age in years.\",\n"
                            + "      \"type\": \"integer\"\n"
                            + "    },\n"
                            + "    \"birthDate\" : {\n"
                            + "      \"type\": \"string\",\n"
                            + "      \"format\": \"date\"\n"
                            + "    },\n"
                            + "    \"createTime\" : {\n"
                            + "      \"type\": \"string\",\n"
                            + "      \"format\": \"date-time\"\n"
                            + "    },\n"
                            + "    \"location\": {\n"
                            + "      \"title\": \"Location\",\n"
                            + "      \"description\": \"Location\",\n"
                            + "      \"type\": \"object\",\n"
                            + "      \"properties\": {\n"
                            + "        \"latitude\": {\n"
                            + "          \"type\": \"number\",\n"
                            + "          \"minimum\": -90,\n"
                            + "          \"maximum\": 90\n"
                            + "        },\n"
                            + "        \"longitude\": {\n"
                            + "          \"type\": \"number\",\n"
                            + "          \"minimum\": -180,\n"
                            + "          \"maximum\": 180\n"
                            + "        },\n"
                            + "        \"city\": {\n"
                            + "          \"title\": \"City\",\n"
                            + "           \"description\": \"City\",\n"
                            + "           \"type\": \"object\",\n"
                            + "           \"properties\": {\n"
                            + "             \"name\": {\n"
                            + "              \"type\": \"string\"\n"
                            + "              },\n"
                            + "             \"country\": {\n"
                            + "               \"type\": \"string\"\n"
                            + "             }\n"
                            + "           }\n"
                            + "        },\n"
                            + "        \"fruits\": {\n"
                            + "          \"type\": \"array\",\n"
                            + "          \"items\": {\n"
                            + "            \"type\": \"object\",\n"
                            + "            \"properties\": {\n"
                            + "              \"family\": {\n"
                            + "                \"type\": \"string\"\n"
                            + "              },\n"
                            + "              \"name\": {\n"
                            + "                \"type\": \"string\"\n"
                            + "              }\n"
                            + "            }\n"
                            + "          }\n"
                            + "        }\n"
                            + "      }\n"
                            + "    }\n"
                            + "  }\n"
                            + "}");
    public static final String TABLE2MESSAGE =
            "{\"name\":\"Abcd\",\"age\":30,\"birthDate\":\"1985-01-01\",\"createTime\":\"2022-08-04 19:00:00\",\"location\":{\"latitude\":24.3,\"longitude\":25.4,\"city\":{\"name\":\"istanbul\",\"country\":\"turkiye\"},\"fruits\":[{\"family\":\"pome\",\"name\":\"apple\"},{\"family\":\"pome\",\"name\":\"apple\"}]}}";
}
