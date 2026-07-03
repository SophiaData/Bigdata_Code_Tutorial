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

package io.sophiadata.flink.sync.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MysqlUtilTest {

    @Test
    void createTable_emitsExpectedColumnsAndPrimaryKey() {
        DataType[] types = {
            DataTypes.BIGINT().notNull(), DataTypes.VARCHAR(255), DataTypes.TINYINT()
        };
        String sql =
                MysqlUtil.createTable(
                        "sink_t_user",
                        new String[] {"id", "name", "age"},
                        types,
                        Collections.singletonList("id"));

        assertTrue(sql.startsWith("create table if not exists `sink_t_user` ("), sql);
        assertTrue(sql.contains("`id` BIGINT NOT NULL"), sql);
        assertTrue(sql.contains("`name` VARCHAR(255)"), sql);
        assertTrue(sql.contains("`age` TINYINT"), sql);
        assertTrue(sql.contains("PRIMARY KEY (id)"), sql);
        assertFalse(sql.contains("NOT ENFORCED"), sql);
        // No TIMESTAMP column in this case -> no 1970 default injected.
        assertFalse(sql.contains("1970-01-01 09:00:00"), sql);
    }

    @Test
    void createTable_injectsTimestampDefaultForTimestampColumns() {
        DataType[] types = {
            DataTypes.BIGINT().notNull(), DataTypes.TIMESTAMP(), DataTypes.TIMESTAMP(3)
        };
        String sql =
                MysqlUtil.createTable(
                        "sink_t_event",
                        new String[] {"id", "create_time", "update_time"},
                        types,
                        Collections.singletonList("id"));

        // Two TIMESTAMP columns each get the 1970 default; BIGINT must not.
        int timestampDefaultOccurrences = countOccurrences(sql, "1970-01-01 09:00:00");
        assertEquals(
                2,
                timestampDefaultOccurrences,
                "expected exactly two 1970 defaults, got "
                        + timestampDefaultOccurrences
                        + ": "
                        + sql);
        assertTrue(sql.contains("`create_time` TIMESTAMP(6)"), sql);
        assertTrue(sql.contains("`update_time` TIMESTAMP(3)"), sql);
        assertFalse(sql.contains("`id` BIGINT NOT NULL default"), sql);
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }

    @Test
    void createTable_rejectsSqlInjectionAttemptInTableName() {
        DataType[] types = {DataTypes.BIGINT()};
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        MysqlUtil.createTable(
                                "t`; DROP TABLE x; --",
                                new String[] {"id"},
                                types,
                                Collections.singletonList("id")));
    }

    @Test
    void createTable_rejectsSqlInjectionAttemptInColumnName() {
        DataType[] types = {DataTypes.BIGINT(), DataTypes.VARCHAR(10)};
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        MysqlUtil.createTable(
                                "t",
                                new String[] {"id", "name;DROP"},
                                types,
                                Collections.singletonList("id")));
    }

    @Test
    void createTable_noPrimaryKey_generatesValidSql() {
        DataType[] types = {DataTypes.BIGINT(), DataTypes.VARCHAR(10)};
        String sql =
                MysqlUtil.createTable(
                        "t", new String[] {"id", "name"}, types, Collections.emptyList());
        assertTrue(sql.contains("`id` BIGINT"), sql);
        assertTrue(sql.contains("`name` VARCHAR(10)"), sql);
        assertFalse(sql.contains("PRIMARY KEY"), sql);
    }

    @Test
    void createTable_rejectsLengthMismatch() {
        DataType[] types = {DataTypes.BIGINT(), DataTypes.VARCHAR(10)};
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        MysqlUtil.createTable(
                                "t", new String[] {"id"}, types, Collections.singletonList("id")));
    }

    @Test
    void createTable_compositePrimaryKey() {
        DataType[] types = {DataTypes.BIGINT(), DataTypes.INT()};
        String sql =
                MysqlUtil.createTable(
                        "sink_t_rel",
                        new String[] {"uid", "gid"},
                        types,
                        Arrays.asList("uid", "gid"));
        assertTrue(sql.contains("PRIMARY KEY (uid,gid)"), sql);
    }
}
