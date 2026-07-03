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

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MongoDocumentFlattenerTest {

    @Test
    void flattenSimpleDocument() {
        Document doc = new Document("name", "Alice").append("age", 30);
        String[] columns = {"name", "age"};
        Object[] result = MongoDocumentFlattener.flatten(doc, columns, 0);
        assertArrayEquals(new Object[] {"Alice", 30}, result);
    }

    @Test
    void flattenWithNestedDocumentAsJson() {
        Document address = new Document("city", "Beijing").append("zip", "100000");
        Document doc = new Document("name", "Bob").append("address", address);
        String[] columns = {"name", "address"};
        Object[] result = MongoDocumentFlattener.flatten(doc, columns, 0);
        assertEquals("Bob", result[0]);
        String json = (String) result[1];
        assertEquals("Beijing", Document.parse(json).getString("city"));
    }

    @Test
    void flattenWithNestedDocumentFlattened() {
        Document address = new Document("city", "Shanghai").append("zip", "200000");
        Document doc = new Document("name", "Charlie").append("address", address);
        String[] columns = {"name", "address.city", "address.zip"};
        Object[] result = MongoDocumentFlattener.flatten(doc, columns, 1);
        assertArrayEquals(new Object[] {"Charlie", "Shanghai", "200000"}, result);
    }

    @Test
    void flattenWithArrayAsJson() {
        List<String> tags = Arrays.asList("java", "flink", "paimon");
        Document doc = new Document("name", "Dave").append("tags", tags);
        String[] columns = {"name", "tags"};
        Object[] result = MongoDocumentFlattener.flatten(doc, columns, 0);
        assertEquals("Dave", result[0]);
        assertEquals("[java, flink, paimon]", result[1]);
    }

    @Test
    void flattenHandlesNullValues() {
        Document doc = new Document("name", "Eve").append("email", null);
        String[] columns = {"name", "email"};
        Object[] result = MongoDocumentFlattener.flatten(doc, columns, 0);
        assertEquals("Eve", result[0]);
        assertNull(result[1]);
    }

    @Test
    void flattenHandlesMissingFields() {
        Document doc = new Document("name", "Frank");
        String[] columns = {"name", "missing"};
        Object[] result = MongoDocumentFlattener.flatten(doc, columns, 0);
        assertEquals("Frank", result[0]);
        assertNull(result[1]);
    }
}
