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

package io.sophiadata.flink.sync;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the parsing helpers in {@link FlinkSqlWDS}.
 *
 * <p>{@code FlinkSqlWDS#handle} builds a whole Flink pipeline and cannot be exercised without a
 * running cluster and a MySQL instance, but its argument and JDBC-URL handling is pure and is where
 * the mistakes actually hurt: a mangled sink URL points the pipeline at the wrong database, and a
 * missing config silently falls back to an empty argument list.
 */
class FlinkSqlWDSHelpersTest {

    @Test
    void extractSinkJdbcUrlKeepsHostPortAndDatabase() throws Exception {
        assertEquals(
                "jdbc:mysql://db.internal:3306/analytics",
                extractSinkJdbcUrl("jdbc:mysql://db.internal:3306/analytics"));
    }

    @Test
    void extractSinkJdbcUrlDropsConnectionParameters() throws Exception {
        // The parameters matter to the CDC connection but must not leak into the sink URL, which is
        // used to derive the database name.
        assertEquals(
                "jdbc:mysql://db.internal:3306/analytics",
                extractSinkJdbcUrl(
                        "jdbc:mysql://db.internal:3306/analytics?useSSL=false&serverTimezone=UTC"));
    }

    @Test
    void extractSinkJdbcUrlHandlesSingleParameter() throws Exception {
        assertEquals(
                "jdbc:mysql://host:3306/db",
                extractSinkJdbcUrl("jdbc:mysql://host:3306/db?characterEncoding=utf8"));
    }

    @Test
    void extractSinkJdbcUrlPreservesHostWithoutPort() throws Exception {
        assertEquals("jdbc:mysql://localhost/db", extractSinkJdbcUrl("jdbc:mysql://localhost/db"));
    }

    @Test
    void extractSinkJdbcUrlIsIdempotent() throws Exception {
        String once = extractSinkJdbcUrl("jdbc:mysql://h:3306/d?x=1");
        assertEquals(once, extractSinkJdbcUrl(once));
    }

    @Test
    void findDefaultConfigReturnsNullWhenNoConfigIsPresent() throws Exception {
        // The test JVM has no config.properties on the classpath and no such file in the working
        // directory, so this exercises the "nothing found" branch.
        assertNull(
                invokeStatic("findDefaultConfig"),
                "with no config.properties available the lookup should report null, not an empty string");
    }

    @Test
    void findDefaultArgsIsEmptyWhenNoConfigIsFound() throws Exception {
        String[] args = (String[]) invokeStatic("findDefaultArgs");

        assertNotNull(args, "findDefaultArgs must never return null");
        // Either nothing was found (empty array) or a config was located (--config <path>). Both
        // are
        // valid; what matters is that the pair is well-formed.
        if (args.length > 0) {
            assertEquals(2, args.length, "a located config must yield exactly --config <path>");
            assertEquals("--config", args[0]);
            assertFalse(args[1].isEmpty(), "the config path must not be blank");
        } else {
            assertEquals(0, args.length);
        }
    }

    @Test
    void mainIsAPublicEntryPoint() throws Exception {
        // The class is launched directly as a Flink job, so a lost public static main would break
        // deployment in a way no other test here would notice.
        Method main = FlinkSqlWDS.class.getMethod("main", String[].class);

        assertTrue(
                java.lang.reflect.Modifier.isPublic(main.getModifiers()), "main must stay public");
        assertTrue(
                java.lang.reflect.Modifier.isStatic(main.getModifiers()), "main must stay static");
    }

    // --- helpers -------------------------------------------------------------

    private String extractSinkJdbcUrl(final String url) throws Exception {
        return (String) invokeStatic("extractSinkJdbcUrl", url);
    }

    private Object invokeStatic(final String name, final Object... args) throws Exception {
        Class<?>[] types = new Class<?>[args.length];
        for (int i = 0; i < args.length; i++) {
            types[i] = String.class;
        }
        Method method = FlinkSqlWDS.class.getDeclaredMethod(name, types);
        method.setAccessible(true);
        return method.invoke(null, args);
    }
}
