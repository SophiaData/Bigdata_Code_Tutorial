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
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for MongoDB components using Testcontainers.
 *
 * <p>Starts a real MongoDB instance and verifies: connection, data operations, type mapping, and
 * document flattening with real data.
 *
 * <p>For full Flink pipeline testing (MongoDB → Paimon), use Docker Compose with the provided
 * docker-compose.yml.
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MongoPaimonEndToEndIT {

    @Container
    private static final MongoDBContainer MONGO =
            new MongoDBContainer(DockerImageName.parse("mongo:6.0")).withExposedPorts(27017);

    private static MongoClient client;
    private static MongoDatabase database;

    @BeforeAll
    static void setUp() {
        client = MongoClients.create(MONGO.getConnectionString());
        database = client.getDatabase("e2e_db");
    }

    @AfterAll
    static void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    @Order(1)
    void connectToMongoDB() {
        MongoCollection<Document> users = database.getCollection("users");
        users.drop();

        users.insertOne(new Document("_id", "u1").append("name", "Alice").append("age", 30));
        users.insertOne(new Document("_id", "u2").append("name", "Bob").append("age", 25));
        users.insertOne(new Document("_id", "u3").append("name", "Charlie").append("age", 35));

        assertEquals(3, users.countDocuments());
    }

    @Test
    @Order(2)
    void verifyDataConsistency() {
        MongoCollection<Document> users = database.getCollection("users");
        List<Document> results = users.find().into(new ArrayList<>());

        assertEquals(3, results.size());

        boolean hasAlice = results.stream().anyMatch(d -> "Alice".equals(d.getString("name")));
        boolean hasBob = results.stream().anyMatch(d -> "Bob".equals(d.getString("name")));
        boolean hasCharlie = results.stream().anyMatch(d -> "Charlie".equals(d.getString("name")));

        assertTrue(hasAlice, "Should contain Alice");
        assertTrue(hasBob, "Should contain Bob");
        assertTrue(hasCharlie, "Should contain Charlie");
    }

    @Test
    @Order(3)
    void insertAndQueryMoreData() {
        MongoCollection<Document> users = database.getCollection("users");
        users.insertOne(new Document("_id", "u4").append("name", "Diana").append("age", 28));

        assertEquals(4, users.countDocuments());

        Document diana = users.find(new Document("_id", "u4")).first();
        assertTrue(diana != null, "Diana should exist");
        assertEquals("Diana", diana.getString("name"));
        assertEquals(28, diana.getInteger("age"));
    }

    @Test
    @Order(4)
    void testTypeMapperWithRealData() {
        MongoCollection<Document> users = database.getCollection("users");
        Document user = users.find(new Document("_id", "u1")).first();

        assertTrue(user != null);
        assertEquals("Alice", user.getString("name"));
        assertEquals(30, user.getInteger("age"));

        assertEquals(
                "STRING",
                MongoTypeMapper.mapType(
                                user.get("_id").getClass() == org.bson.types.ObjectId.class
                                        ? org.bson.BsonType.OBJECT_ID
                                        : org.bson.BsonType.STRING)
                        .toString());
    }

    @Test
    @Order(5)
    void testFlattenerWithRealData() {
        MongoCollection<Document> users = database.getCollection("users");
        Document user = users.find(new Document("_id", "u1")).first();

        String[] columns = {"_id", "name", "age"};
        Object[] flattened = MongoDocumentFlattener.flatten(user, columns, 0);

        assertEquals(3, flattened.length);
        assertEquals("Alice", flattened[1]);
        assertEquals(30, flattened[2]);
    }

    @Test
    @Order(6)
    void testConnectionInfo() {
        String connectionString = MONGO.getConnectionString();
        assertTrue(connectionString.startsWith("mongodb://"), "Should start with mongodb://");
        assertTrue(
                connectionString.contains("localhost") || connectionString.contains("127.0.0.1"),
                "Should contain host");
    }
}
