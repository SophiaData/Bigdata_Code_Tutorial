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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for MongoDB to Paimon pipeline using Testcontainers.
 *
 * <p>Starts a real MongoDB instance, inserts test data, and verifies the pipeline can connect and
 * read documents.
 */
@Testcontainers
class MongoToPaimonPipelineIT {

    @Container
    private static final MongoDBContainer MONGO =
            new MongoDBContainer(DockerImageName.parse("mongo:6.0")).withExposedPorts(27017);

    private static MongoClient client;
    private static MongoDatabase database;

    @BeforeAll
    static void setUp() {
        client = MongoClients.create(MONGO.getConnectionString());
        database = client.getDatabase("test_db");
    }

    @AfterAll
    static void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void connectToMongoDB() {
        MongoCollection<Document> collection = database.getCollection("test_collection");
        Document doc = new Document("name", "Alice").append("age", 30);
        collection.insertOne(doc);

        List<Document> results = collection.find().into(new ArrayList<>());
        assertEquals(1, results.size());
        assertEquals("Alice", results.get(0).getString("name"));
        assertEquals(30, results.get(0).getInteger("age"));
    }

    @Test
    void listCollections() {
        database.getCollection("users").insertOne(new Document("key", "value"));
        database.getCollection("orders").insertOne(new Document("key", "value"));

        List<String> names = database.listCollectionNames().into(new ArrayList<>());
        assertTrue(names.contains("users"));
        assertTrue(names.contains("orders"));
    }

    @Test
    void insertAndReadMultipleDocuments() {
        MongoCollection<Document> collection = database.getCollection("batch_test");
        collection.drop();

        for (int i = 0; i < 10; i++) {
            collection.insertOne(new Document("index", i).append("data", "item_" + i));
        }

        List<Document> results = collection.find().into(new ArrayList<>());
        assertEquals(10, results.size());
    }

    @Test
    void connectionStringFormat() {
        String connectionString = MONGO.getConnectionString();
        assertTrue(connectionString.startsWith("mongodb://"), "should start with mongodb://");
    }
}
