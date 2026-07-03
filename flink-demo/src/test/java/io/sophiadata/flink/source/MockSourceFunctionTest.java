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

package io.sophiadata.flink.source;

import io.sophiadata.flink.source.bean.AppMain;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MockSourceFunctionTest {

    @Test
    void doAppMock_returnsNonEmptyList() {
        MockSourceFunction source = new MockSourceFunction();
        List<AppMain> result = source.doAppMock();
        assertNotNull(result);
        assertFalse(result.isEmpty(), "doAppMock should return at least one event");
    }

    @Test
    void doAppMock_firstEventIsStart() {
        MockSourceFunction source = new MockSourceFunction();
        List<AppMain> result = source.doAppMock();
        assertNotNull(result.get(0).getStart(), "First event should be an AppStart");
    }

    @Test
    void doAppMock_containsPageEvents() {
        MockSourceFunction source = new MockSourceFunction();
        List<AppMain> result = source.doAppMock();
        boolean hasPages = result.stream().anyMatch(e -> e.getPage() != null);
        assertTrue(hasPages, "Should contain at least one page event");
    }

    private static void assertTrue(boolean condition, String message) {
        org.junit.jupiter.api.Assertions.assertTrue(condition, message);
    }
}
