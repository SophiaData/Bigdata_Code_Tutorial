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

/** (@SophiaData) (@date 2023/3/6 10:28). */
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.kafka.factories.KafkaAdminClientFactory;
import org.apache.flink.table.catalog.kafka.factories.KafkaCatalogFactoryOptions;
import org.apache.flink.table.catalog.kafka.factories.SchemaRegistryClientFactory;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.CATALOG_NAME;
import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.SCHEMA_REGISTRY_URIS;
import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.TABLE1;
import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.TABLE1AVROSCHEMA;
import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.TABLE1PATH;
import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.TABLE2;
import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.TABLE2JSONSCHEMA;
import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.TABLE2PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** */
public class KafkaCatalogTest {

    private KafkaCatalog catalog;

    @Before
    public void init() throws RestClientException, IOException {
        SchemaRegistryClientFactory schemaRegistryClientFactory = new SchemaRegistryClientFactory();
        SchemaRegistryClient schemaRegistryClient =
                schemaRegistryClientFactory.get(SCHEMA_REGISTRY_URIS, 1000, new HashMap<>());
        schemaRegistryClient.register(TABLE1, TABLE1AVROSCHEMA);
        schemaRegistryClient.register(TABLE2, TABLE2JSONSCHEMA);

        Map<String, String> properties = new HashMap<>();
        properties.put("connector", "kafka");
        properties.put(
                KafkaCatalogFactoryOptions.SCHEMA_REGISTRY_URI.key(),
                String.join(", ", SCHEMA_REGISTRY_URIS));

        KafkaAdminClientFactory mockAdminClientFactory = mock(KafkaAdminClientFactory.class);
        AdminClient mockAdminClient = mock(AdminClient.class);
        ListTopicsResult mockListTopicsResult = mock(ListTopicsResult.class);
        when(mockListTopicsResult.names())
                .thenReturn(
                        KafkaFuture.completedFuture(new HashSet<>(Arrays.asList(TABLE1, TABLE2))));
        when(mockAdminClient.listTopics(any())).thenReturn(mockListTopicsResult);
        when(mockAdminClientFactory.get(any())).thenReturn(mockAdminClient);
        catalog = new KafkaCatalog(CATALOG_NAME, properties, mockAdminClientFactory);
        catalog.open();
    }

    @Test
    public void testDefaultDBExists() {
        assertTrue(catalog.databaseExists(KafkaCatalog.DEFAULT_DB));
    }

    @Test
    public void testNonDefaultDBDoesNotExists() {
        assertFalse(catalog.databaseExists("Test"));
    }

    @Test
    public void testGetDbExist() throws Exception {
        CatalogDatabase db = catalog.getDatabase(KafkaCatalog.DEFAULT_DB);
        assertNotNull(db);
    }

    @Test(expected = TableNotExistException.class)
    public void testGetTableNotExist() throws Exception {
        catalog.getTable(new ObjectPath(KafkaCatalog.DEFAULT_DB, "NOT_EXIST"));
    }

    @Test
    public void testListTables() throws Exception {
        List<String> tables = catalog.listTables(KafkaCatalog.DEFAULT_DB);

        assertEquals(2, tables.size());
        assertTrue(tables.contains(TABLE1));
        assertTrue(tables.contains(TABLE2));
    }

    @Test
    public void testTableExists() {
        assertTrue(catalog.tableExists(TABLE1PATH));
        assertTrue(catalog.tableExists(TABLE2PATH));
    }

    @Test
    public void testTableNotExists() {
        assertFalse(catalog.tableExists(new ObjectPath(KafkaCatalog.DEFAULT_DB, "NOT_EXIST")));
    }

    @Test
    public void testAvroGetTable() throws TableNotExistException {
        CatalogTable table = (CatalogTable) catalog.getTable(TABLE1PATH);
        Schema schema = table.getUnresolvedSchema();
        Map<String, String> options = table.getOptions();
        assertEquals("avro-confluent", options.get("format"));
        assertEquals(TABLE1, options.get("topic"));
        assertEquals("kafka", options.get("connector"));
        assertEquals(2, schema.getColumns().size());
        Optional<Schema.UnresolvedColumn> column =
                schema.getColumns().stream()
                        .filter(c -> Objects.equals(c.getName(), "name"))
                        .findFirst();
        assertTrue(column.isPresent());
    }

    @Test
    public void testJsonGetTable() throws TableNotExistException {
        CatalogTable table = (CatalogTable) catalog.getTable(TABLE2PATH);
        Schema schema = table.getUnresolvedSchema();
        Map<String, String> options = table.getOptions();
        assertEquals("json", options.get("format"));
        assertEquals(TABLE2, options.get("topic"));
        assertEquals("kafka", options.get("connector"));
        assertEquals(5, schema.getColumns().size());
        Optional<Schema.UnresolvedColumn> column =
                schema.getColumns().stream()
                        .filter(c -> Objects.equals(c.getName(), "name"))
                        .findFirst();
        assertTrue(column.isPresent());
    }
}
