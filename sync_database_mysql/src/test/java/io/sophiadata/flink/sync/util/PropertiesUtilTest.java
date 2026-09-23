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

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link PropertiesUtil}.
 *
 * <p>Previously this class mixed a JUnit 4 {@code @Test} annotation with JUnit 5 assertions, which
 * meant the JUnit Platform could silently skip it (JUnit 4's {@code @Test} is not discoverable
 * without the vintage engine). It is now uniformly JUnit 5.
 */
class PropertiesUtilTest {

    @Test
    void testLoadSuccess() {
        String content = "k1=v1\nk2=v2";
        Properties props = PropertiesUtil.load(content);
        assertEquals("v1", props.getProperty("k1"));
        assertEquals("v2", props.getProperty("k2"));
    }

    @Test
    void testLoadWithNullContent() {
        assertThrows(IllegalArgumentException.class, () -> PropertiesUtil.load(null));
    }

    @Test
    void testLoadWithEmptyContent() {
        Properties props = PropertiesUtil.load("");
        assertEquals(0, props.size());
    }
}
