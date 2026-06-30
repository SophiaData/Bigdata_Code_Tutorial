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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * MongoDB to Paimon sync using Flink SQL Catalog API.
 *
 * <p>This approach creates a MongoDB catalog and a Paimon catalog, then uses {@code INSERT INTO}
 * statements to sync data. Simpler than the DataStream approach but requires the target Paimon
 * tables to be pre-created or managed separately.
 *
 * <p>Usage:
 *
 * <pre>
 * flink run -c io.sophiadata.flink.paimon.mongo.MongoToPaimonSqlPipeline \
 *   flink-paimon-demo-1.0.0.jar \
 *   --mongo.host localhost --mongo.port 27017 --mongo.database source_db \
 *   --mongo.username root --mongo.password root --paimon.path /path/to/paimon \
 *   --mongo.collections "users,orders"
 * </pre>
 */
@SuppressWarnings("PMD.UseUtilityClass")
public class MongoToPaimonSqlPipeline {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String mongoHost = params.get("mongo.host", "localhost");
        int mongoPort = params.getInt("mongo.port", 27017);
        String mongoDatabase = params.get("mongo.database");
        String mongoUsername = params.get("mongo.username", "root");
        String mongoPassword = params.get("mongo.password", "root");
        String paimonPath = params.get("paimon.path", "file:///tmp/paimon/catalog");
        String collectionsParam = params.get("mongo.collections", "");

        if (mongoDatabase == null || mongoDatabase.isEmpty()) {
            throw new IllegalArgumentException("--mongo.database is required");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. Create MongoDB catalog
        tableEnv.executeSql(
                "CREATE CATALOG mongo_catalog WITH ("
                        + "  'type' = 'mongodb',"
                        + "  'connection.uri' = 'mongodb://"
                        + mongoUsername
                        + ":"
                        + mongoPassword
                        + "@"
                        + mongoHost
                        + ":"
                        + mongoPort
                        + "'"
                        + ")");

        // 2. Create Paimon catalog
        tableEnv.executeSql(
                "CREATE CATALOG paimon_catalog WITH ("
                        + "  'type' = 'paimon',"
                        + "  'warehouse' = '"
                        + paimonPath
                        + "'"
                        + ")");

        // 3. Create target database
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS paimon_catalog." + mongoDatabase);

        // 4. Sync collections
        if (collectionsParam.isEmpty()) {
            throw new IllegalArgumentException("--mongo.collections is required for SQL mode");
        }
        String[] collections = collectionsParam.split(",");
        for (String collection : collections) {
            String trimmed = collection.trim();
            tableEnv.executeSql(
                    "INSERT INTO paimon_catalog."
                            + mongoDatabase
                            + "."
                            + trimmed
                            + " SELECT * FROM mongo_catalog."
                            + mongoDatabase
                            + "."
                            + trimmed);
        }

        env.execute("MongoDB SQL to Paimon Sync - " + mongoDatabase);
    }
}
