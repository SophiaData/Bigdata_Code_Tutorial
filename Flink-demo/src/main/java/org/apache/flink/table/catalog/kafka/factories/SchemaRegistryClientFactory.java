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

package org.apache.flink.table.catalog.kafka.factories;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** (@SophiaData) (@date 2023/3/6 09:47). */
public class SchemaRegistryClientFactory {
    public SchemaRegistryClientFactory() {}

    public SchemaRegistryClient get(
            List<String> urls, int maxSchemaObject, Map<String, String> properties) {
        List<SchemaProvider> providers =
                Arrays.asList(new AvroSchemaProvider(), new JsonSchemaProvider());
        return get(urls, maxSchemaObject, properties, providers);
    }

    public SchemaRegistryClient get(
            List<String> urls,
            int maxSchemaObject,
            Map<String, String> properties,
            List<SchemaProvider> providers) {
        String mockScope = MockSchemaRegistry.validateAndMaybeGetMockScope(urls);
        if (mockScope != null) {
            return MockSchemaRegistry.getClientForScope(mockScope, providers);
        } else {
            return new CachedSchemaRegistryClient(urls, maxSchemaObject, providers, properties);
        }
    }
}
