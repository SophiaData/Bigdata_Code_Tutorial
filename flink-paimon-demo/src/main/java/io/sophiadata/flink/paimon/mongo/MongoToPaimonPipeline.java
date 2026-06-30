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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.mongodb.MongoDBSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * MongoDB CDC to Apache Paimon sync pipeline using DataStream API.
 *
 * <p>Reads change events from MongoDB via CDC connector and produces {@link Document} streams that
 * can be consumed by downstream Paimon sinks.
 *
 * <p>Usage:
 *
 * <pre>
 *   flink run -c io.sophiadata.flink.paimon.mongo.MongoToPaimonPipeline \
 *     flink-paimon-demo-1.0.0.jar \
 *     --mongo.host localhost --mongo.port 27017 --mongo.database source_db \
 *     --mongo.username root --mongo.password root --paimon.path /path/to/paimon \
 *     --mongo.collections "users,orders"
 * </pre>
 */
public final class MongoToPaimonPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(MongoToPaimonPipeline.class);

    private MongoToPaimonPipeline() {}

    public static void main(final String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final String mongoHost = params.get("mongo.host", "localhost");
        final int mongoPort = params.getInt("mongo.port", 27017);
        final String mongoDatabase = params.get("mongo.database");
        final String mongoUsername = params.get("mongo.username", "root");
        final String mongoPassword = params.get("mongo.password", "root");
        final String collectionsParam = params.get("mongo.collections", "");
        final String paimonPath = params.get("paimon.path", "file:///tmp/paimon/catalog");

        if (mongoDatabase == null || mongoDatabase.isEmpty()) {
            throw new IllegalArgumentException("--mongo.database is required");
        }

        String[] collections;
        if (collectionsParam.isEmpty()) {
            collections =
                    listCollections(
                            mongoHost, mongoPort, mongoDatabase, mongoUsername, mongoPassword);
        } else {
            collections = collectionsParam.split(",");
        }

        LOG.info(
                "Syncing {} collections from {}: {}",
                collections.length,
                mongoDatabase,
                String.join(", ", collections));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Create Paimon catalog
        tableEnv.executeSql(
                "CREATE CATALOG paimon_catalog WITH ("
                        + "  'type' = 'paimon',"
                        + "  'warehouse' = '"
                        + paimonPath
                        + "'"
                        + ")");

        // Create target database
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS paimon_catalog." + mongoDatabase);

        for (final String collection : collections) {
            final String trimmed = collection.trim();
            final DataStream<Document> stream =
                    env.addSource(
                                    MongoDBSource.<Document>builder()
                                            .scheme("mongodb")
                                            .hosts(mongoHost + ":" + mongoPort)
                                            .username(mongoUsername)
                                            .password(mongoPassword)
                                            .databaseList(mongoDatabase)
                                            .collectionList(trimmed)
                                            .deserializer(new MongoDBDebeziumDeserializer())
                                            .build(),
                                    "mongo-" + trimmed)
                            .uid("mongo-" + trimmed);

            stream.process(
                            new ProcessFunction<Document, Document>() {
                                @Override
                                public void processElement(
                                        final Document doc,
                                        final Context ctx,
                                        final Collector<Document> out) {
                                    out.collect(doc);
                                }
                            })
                    .name("process-" + trimmed)
                    .uid("process-" + trimmed);

            // Register as temp view and insert into Paimon
            tableEnv.createTemporaryView("mongo_" + trimmed, stream);
            tableEnv.executeSql(
                    "INSERT INTO paimon_catalog."
                            + mongoDatabase
                            + "."
                            + trimmed
                            + " SELECT * FROM mongo_"
                            + trimmed);
        }

        env.execute("MongoDB to Paimon Sync - " + mongoDatabase);
    }

    private static String[] listCollections(
            final String host,
            final int port,
            final String database,
            final String username,
            final String password) {
        final String connectionString =
                String.format("mongodb://%s:%s@%s:%d/%s", username, password, host, port, database);
        try (MongoClient client = MongoClients.create(connectionString)) {
            final MongoDatabase db = client.getDatabase(database);
            final List<String> names = db.listCollectionNames().into(new ArrayList<>());
            return names.toArray(new String[0]);
        }
    }
}
