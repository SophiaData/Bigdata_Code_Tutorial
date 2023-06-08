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

package org.apache.flink.table.catalog.kafka.json;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.json.JsonSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** (@SophiaData) (@date 2023/3/6 09:49). */
public class JsonSchemaConverter {

    private JsonSchemaConverter() {
        // private
    }

    public static DataType convertToDataType(JsonSchema jsonSchema) {
        Preconditions.checkNotNull(jsonSchema, "Json schema must not be null.");
        return convertToDataType(jsonSchema.toJsonNode());
    }

    private static DataType convertToDataType(JsonNode node) {
        String type;
        if (node.isTextual()) {
            type = node.asText();
        } else {
            JsonNode typeNode = node.get("type");
            // Composite schema
            if (typeNode.isArray()) {
                List<JsonNode> types = new ArrayList<>();
                typeNode.elements().forEachRemaining(types::add);
                JsonNode actualNode;
                boolean nullable;
                if (types.size() == 2
                        && types.get(0).isTextual()
                        && Objects.equals(types.get(0).asText(), "null")) {
                    actualNode = types.get(1);
                    nullable = true;
                } else if (types.size() == 2
                        && types.get(1).isTextual()
                        && Objects.equals(types.get(1).asText(), "null")) {
                    actualNode = types.get(0);
                    nullable = true;
                } else {
                    if (types.size() != 1) // noinspection RedundantSuppression
                    {
                        //noinspection unchecked,deprecation,rawtypes
                        return new AtomicDataType(
                                new TypeInformationRawType(false, Types.GENERIC(Object.class)));
                    }

                    actualNode = types.get(0);
                    nullable = false;
                }

                DataType converted = convertToDataType(actualNode);
                return nullable ? converted.nullable() : converted;
            }
            type = typeNode.asText("");
        }
        switch (type) {
            case "object":
                if (!node.has("properties")) {
                    throw new IllegalArgumentException(
                            "Json object does not have properties field.");
                }
                JsonNode properties = node.get("properties");
                List<Map.Entry<String, JsonNode>> schemaFields = new ArrayList<>();
                properties.fields().forEachRemaining(schemaFields::add);

                List<String> requiredFields = new ArrayList<>();
                if (node.has("required")) {
                    JsonNode required = node.get("required");
                    required.elements().forEachRemaining(r -> requiredFields.add(r.asText()));
                }
                DataTypes.Field[] fields = new DataTypes.Field[schemaFields.size()];
                for (int i = 0; i < schemaFields.size(); ++i) {
                    Map.Entry<String, JsonNode> field = schemaFields.get(i);
                    DataType dataType = convertToDataType(field.getValue());
                    if (!requiredFields.contains(field.getKey())) {
                        dataType = dataType.nullable();
                    }
                    fields[i] = DataTypes.FIELD(field.getKey(), dataType);
                }
                return DataTypes.ROW(fields).notNull();

            case "array":
                if (!node.has("items")) {
                    throw new IllegalArgumentException("Json array does not have items field.");
                }
                return DataTypes.ARRAY(convertToDataType(node.get("items"))).notNull();
                //            case MAP:
                //                return
                // (DataType)DataTypes.MAP((DataType)DataTypes.STRING().notNull(),
                // convertToDataType(schema.getValueType())).notNull();
            case "string":
                if (node.has("format")) {
                    String format = node.get("format").asText();
                    if (Objects.equals(format, "date")) {
                        return DataTypes.DATE().notNull();
                    } else if (Objects.equals(format, "date-time")) {
                        return DataTypes.TIMESTAMP().notNull();
                    } else if (Objects.equals(format, "datetime-local")) {
                        return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().notNull();
                    } else if (Objects.equals(format, "time")) {
                        return DataTypes.TIME().notNull();
                    }
                }
                return DataTypes.STRING().notNull();
            case "integer":
                return DataTypes.INT().notNull();
            case "number":
                return DataTypes.DECIMAL(38, 8).notNull();
            case "boolean":
                return DataTypes.BOOLEAN().notNull();
            case "null":
                return DataTypes.NULL();
            default:
                throw new IllegalArgumentException("Unsupported Json type '" + type + "'.");
        }
    }
}
