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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.kafka.KafkaCatalog;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** (@SophiaData) (@date 2023/3/6 10:19). */
public class KafkaCatalogFactoryTest {
    private static final List<String> SCHEMA_REGISTRY_URIS = Collections.singletonList("mock://");
    private static final String CATALOG_NAME = "TEST_CATALOG";

    @Test
    public void testCreateCatalogFromFactory() {
        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), KafkaCatalogFactoryOptions.IDENTIFIER);
        options.put(KafkaCatalogFactoryOptions.BOOTSTRAP_SERVERS.key(), "kafka");
        options.put(
                KafkaCatalogFactoryOptions.SCHEMA_REGISTRY_URI.key(),
                String.join(", ", SCHEMA_REGISTRY_URIS));
        options.put("properties.group.id", "test");

        final Catalog actualCatalog =
                FactoryUtil.createCatalog(
                        CATALOG_NAME,
                        options,
                        null,
                        Thread.currentThread().getContextClassLoader());

        assertTrue(actualCatalog instanceof KafkaCatalog);
        assertEquals(((KafkaCatalog) actualCatalog).getName(), CATALOG_NAME);
        assertEquals(((KafkaCatalog) actualCatalog).getDefaultDatabase(), KafkaCatalog.DEFAULT_DB);
        //        assertEquals(Whitebox.getInternalState(actualCatalog, "properties"),
        //                Whitebox.getInternalState(CATALOG, "properties"));
    }

    @Test
    public void testCreateCatalogFromFactoryFailsIfRegistryURIIsMissing() {
        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), KafkaCatalogFactoryOptions.IDENTIFIER);
        options.put(KafkaCatalogFactoryOptions.BOOTSTRAP_SERVERS.key(), "kafka");

        ValidationException exception =
                assertThrows(
                        ValidationException.class,
                        () ->
                                FactoryUtil.createCatalog(
                                        CATALOG_NAME,
                                        options,
                                        null,
                                        Thread.currentThread().getContextClassLoader()));

        assertTrue(
                exception
                        .getCause()
                        .getMessage()
                        .contains(KafkaCatalogFactoryOptions.SCHEMA_REGISTRY_URI.key()));
    }
}
