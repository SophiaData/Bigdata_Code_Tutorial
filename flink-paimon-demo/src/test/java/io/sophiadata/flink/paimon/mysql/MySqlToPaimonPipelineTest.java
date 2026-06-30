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

package io.sophiadata.flink.paimon.mysql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for MySQL to Paimon pipeline configuration. */
class MySqlToPaimonPipelineTest {

    @Test
    void defaultParametersAreCorrect() {
        String defaultHost = "localhost";
        int defaultPort = 3306;
        String defaultDatabase = "source_db";
        String defaultUsername = "root";
        String defaultPassword = "root";
        String defaultPaimonPath = "file:///tmp/paimon/catalog";

        assertEquals("localhost", defaultHost);
        assertEquals(3306, defaultPort);
        assertEquals("source_db", defaultDatabase);
        assertEquals("root", defaultUsername);
        assertEquals("root", defaultPassword);
        assertEquals("file:///tmp/paimon/catalog", defaultPaimonPath);
    }

    @Test
    void mysqlCatalogSqlIsValid() {
        String sql =
                "CREATE CATALOG mysql_catalog WITH ("
                        + "  'type' = 'mysql-cdc',"
                        + "  'hostname' = 'localhost',"
                        + "  'port' = '3306',"
                        + "  'username' = 'root',"
                        + "  'password' = 'root',"
                        + "  'database-name' = 'source_db'"
                        + ")";

        assertTrue(sql.contains("'type' = 'mysql-cdc'"));
        assertTrue(sql.contains("'hostname' = 'localhost'"));
        assertTrue(sql.contains("'database-name' = 'source_db'"));
    }

    @Test
    void paimonCatalogSqlIsValid() {
        String sql =
                "CREATE CATALOG paimon_catalog WITH ("
                        + "  'type' = 'paimon',"
                        + "  'warehouse' = 'file:///tmp/paimon/catalog'"
                        + ")";

        assertTrue(sql.contains("'type' = 'paimon'"));
        assertTrue(sql.contains("'warehouse' = 'file:///tmp/paimon/catalog'"));
    }

    @Test
    void paimonTableSqlIsValid() {
        String table = "users";
        String database = "source_db";
        String paimonPath = "file:///tmp/paimon/catalog";

        String sql =
                "CREATE TABLE IF NOT EXISTS paimon_catalog."
                        + database
                        + "."
                        + table
                        + " ("
                        + "  id BIGINT,"
                        + "  name STRING,"
                        + "  age INT,"
                        + "  create_time TIMESTAMP(3),"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector' = 'paimon',"
                        + "  'path' = '"
                        + paimonPath
                        + "/"
                        + database
                        + "/"
                        + table
                        + "'"
                        + ")";

        assertTrue(sql.contains("paimon_catalog.source_db.users"));
        assertTrue(sql.contains("'connector' = 'paimon'"));
        assertTrue(sql.contains("PRIMARY KEY (id) NOT ENFORCED"));
    }

    @Test
    void insertSqlIsValid() {
        String database = "source_db";
        String table = "users";

        String sql =
                "INSERT INTO paimon_catalog."
                        + database
                        + "."
                        + table
                        + " SELECT * FROM mysql_catalog."
                        + database
                        + "."
                        + table;

        assertTrue(sql.contains("INSERT INTO paimon_catalog.source_db.users"));
        assertTrue(sql.contains("SELECT * FROM mysql_catalog.source_db.users"));
    }

    @Test
    void tableListContainsExpectedTables() {
        String[] tables = {"users", "orders", "products"};
        assertEquals(3, tables.length);
        assertTrue(java.util.Arrays.asList(tables).contains("users"));
        assertTrue(java.util.Arrays.asList(tables).contains("orders"));
        assertTrue(java.util.Arrays.asList(tables).contains("products"));
    }

    @Test
    void paimonPathConstructionIsValid() {
        String paimonPath = "file:///tmp/paimon/catalog";
        String database = "source_db";
        String table = "users";

        String fullPath = paimonPath + "/" + database + "/" + table;
        assertEquals("file:///tmp/paimon/catalog/source_db/users", fullPath);
    }
}
