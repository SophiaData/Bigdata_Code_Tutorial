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

import org.apache.flink.api.java.utils.ParameterTool;

import io.sophiadata.flink.sync.common.Constants;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ParameterUtilTest {

    @Test
    public void testValidParameters() {
        // Create a ParameterTool instance with valid parameters
        String[] args = {
                "--sinkUrl", "jdbc:mysql://localhost:3306/mydb",
                "--sinkUsername", "user",
                "--sinkPassword", "password",
                "--hostname", "localhost",
                "--port", "5432",
                "--username", "user",
                "--password", "password",
                "--databaseName", "mydb",
                "--tableList", "table1,table2",
                "--setParallelism", "4",
                "--cdcSourceName", "source"
        };
        ParameterTool params = ParameterTool.fromArgs(args);

        // Test each parameter retrieval method
        assertEquals("jdbc:mysql://localhost:3306/mydb", ParameterUtil.sinkUrl(params));
        assertEquals("user", ParameterUtil.sinkUsername(params));
        assertEquals("password", ParameterUtil.sinkPassword(params));
        assertEquals("localhost", ParameterUtil.hostname(params));
        assertEquals(5432, ParameterUtil.port(params));
        assertEquals("user", ParameterUtil.username(params));
        assertEquals("password", ParameterUtil.password(params));
        assertEquals("mydb", ParameterUtil.databaseName(params));
        assertEquals("table1,table2", ParameterUtil.tableList(params));
        assertEquals(4, ParameterUtil.setParallelism(params));
        assertEquals("source", ParameterUtil.cdcSourceName(params));
    }

    @Test
    public void testMissingParameters() {
        // Create a ParameterTool instance without any parameters
        String[] args = {};
        ParameterTool params = ParameterTool.fromArgs(args);

        // Test each parameter retrieval method with missing parameters
        // Ensure that they return the default values from Constants class
        assertEquals(Constants.sinkUrl, ParameterUtil.sinkUrl(params));
        assertEquals(Constants.sinkUsername, ParameterUtil.sinkUsername(params));
        assertEquals(Constants.sinkPassword, ParameterUtil.sinkPassword(params));
        assertEquals(Constants.hostname, ParameterUtil.hostname(params));
        assertEquals(Constants.port, ParameterUtil.port(params));
        assertEquals(Constants.username, ParameterUtil.username(params));
        assertEquals(Constants.password, ParameterUtil.password(params));
        assertEquals(Constants.databaseName, ParameterUtil.databaseName(params));
        assertEquals(Constants.tableList, ParameterUtil.tableList(params));
        assertEquals(Constants.setParallelism, ParameterUtil.setParallelism(params));
        assertEquals(Constants.cdcSourceName, ParameterUtil.cdcSourceName(params));
    }
}
