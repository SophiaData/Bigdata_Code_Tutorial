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

/** (@SophiaData) (@date 2023/3/6 10:24). */
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.kafka.factories.KafkaAdminClientFactory;
import org.apache.flink.table.catalog.kafka.factories.KafkaCatalogFactoryOptions;
import org.apache.flink.table.catalog.kafka.factories.SchemaRegistryClientFactory;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.github.embeddedkafka.schemaregistry.EmbeddedKafka;
import io.github.embeddedkafka.schemaregistry.EmbeddedKafka$;
import io.github.embeddedkafka.schemaregistry.EmbeddedKafkaConfigImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import scala.collection.immutable.HashMap;
import scala.concurrent.duration.Duration;

import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.CATALOG_NAME;
import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.TABLE1;
import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.TABLE1AVROSCHEMA;
import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.TABLE2;
import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.TABLE2IDENTIFIER;
import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.TABLE2JSONSCHEMA;
import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.TABLE2MESSAGE;
import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.TABLE2_TARGET;
import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.TABLE2_TARGETIDENTIFIER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** */
@Ignore
public class KafkaCatalogIntegrationTest {

    private static EmbeddedKafka$ kafkaOps;
    private static EmbeddedKafkaConfigImpl kafkaConfig;
    private static final int KAFKA_PORT = 6001;
    private static final int ZK_PORT = 6002;
    private static final int SR_PORT = 6003;
    public static final List<String> EMBEDDED_SCHEMA_REGISTRY_URIS =
            Collections.singletonList("http://localhost:" + SR_PORT);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @BeforeClass
    public static void setup() throws RestClientException, IOException {
        kafkaConfig =
                new EmbeddedKafkaConfigImpl(
                        KAFKA_PORT,
                        ZK_PORT,
                        SR_PORT,
                        new HashMap<String, String>(),
                        new HashMap<String, String>(),
                        new HashMap<String, String>(),
                        new HashMap<String, String>());
        EmbeddedKafka.start(kafkaConfig);
        kafkaOps = EmbeddedKafka$.MODULE$;
        kafkaOps.createCustomTopic(TABLE1, new HashMap<String, String>(), 2, 1, kafkaConfig);
        kafkaOps.createCustomTopic(TABLE2, new HashMap<String, String>(), 2, 1, kafkaConfig);
        kafkaOps.createCustomTopic(TABLE2_TARGET, new HashMap<String, String>(), 2, 1, kafkaConfig);

        SchemaRegistryClientFactory schemaRegistryClientFactory = new SchemaRegistryClientFactory();
        SchemaRegistryClient schemaRegistryClient =
                schemaRegistryClientFactory.get(
                        EMBEDDED_SCHEMA_REGISTRY_URIS, 1000, new java.util.HashMap<>());
        schemaRegistryClient.register(TABLE1, TABLE1AVROSCHEMA);
        schemaRegistryClient.register(TABLE2, TABLE2JSONSCHEMA);
        schemaRegistryClient.register(TABLE2_TARGET, TABLE2JSONSCHEMA);
    }

    @AfterClass
    public static void tearDown() {
        EmbeddedKafka.stop();
    }

    @Test
    public void test() {

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tableEnv.getConfig() // access high-level configuration
                .getConfiguration() // set low-level key-value options
                .setString("table.exec.resource.default-parallelism", "1");

        Map<String, String> properties = new java.util.HashMap<>();
        properties.put("connector", "kafka");
        properties.put(
                KafkaCatalogFactoryOptions.BOOTSTRAP_SERVERS.key(),
                "localhost:" + kafkaConfig.kafkaPort());
        properties.put(
                KafkaCatalogFactoryOptions.SCHEMA_REGISTRY_URI.key(),
                String.join(", ", EMBEDDED_SCHEMA_REGISTRY_URIS));
        // Create a Kafka Catalog
        Catalog catalog = new KafkaCatalog("kafka", properties, new KafkaAdminClientFactory());
        // Register the catalog
        tableEnv.registerCatalog(CATALOG_NAME, catalog);

        tableEnv.executeSql(
                "INSERT INTO "
                        + TABLE2IDENTIFIER
                        + "\n"
                        + "SELECT name,age,TO_DATE('1985-01-01') as birthDate,TO_TIMESTAMP('2022-08-04 19:00:00') as createTime,ROW(latitude,longitude, city,fruits) as location\n"
                        + "FROM(\n"
                        + "SELECT *,ROW(city_name,country) as city,ARRAY[ROW('berry','tomato'),ROW('pome','apple')] as fruits\n"
                        + "FROM (VALUES\n"
                        + "        ('Abcd',30,24.3,25.4,'istanbul','turkiye'))\n"
                        + "AS t(name, age,latitude,longitude,city_name,country))");

        //        tableEnv.executeSql("INSERT INTO " + table2_targetIdentifier + "\n" +
        //                "SELECT *\n" +
        //                "FROM " + table2Identifier);
        tableEnv.executeSql(
                "INSERT INTO "
                        + TABLE2_TARGETIDENTIFIER
                        + "(name,age,birthDate,createTime,location)\n"
                        + "SELECT name,age,birthDate,createTime,location\n"
                        + "FROM "
                        + TABLE2IDENTIFIER);

        String record1 =
                kafkaOps.consumeFirstStringMessageFrom(
                        TABLE2, false, Duration.create(120, TimeUnit.SECONDS), kafkaConfig);
        assertNotNull(record1);
        assertEquals(TABLE2MESSAGE, record1);
        String record2 =
                kafkaOps.consumeFirstStringMessageFrom(
                        TABLE2_TARGET, false, Duration.create(60, TimeUnit.SECONDS), kafkaConfig);
        assertNotNull(record2);
        assertEquals(TABLE2MESSAGE, record2);
    }
}
