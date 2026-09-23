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

package io.sophiadata.flink.compat;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParameterTool} and {@link FlinkCompat}.
 *
 * <p>These assert the shim behaves like Flink's own {@code ParameterTool}, because application code
 * was migrated onto it wholesale. In particular the fallback accessors must not throw: the
 * inherited {@code defaultData} and {@code unrequestedParameters} fields are easy to leave
 * uninitialised when subclassing, and a null there only surfaces as a NullPointerException at call
 * time.
 */
class ParameterToolTest {

    @Test
    void fromArgsReadsKeyValuePairs() {
        ParameterTool params = ParameterTool.fromArgs(new String[] {"--hostname", "localhost"});

        assertEquals("localhost", params.get("hostname"));
        assertTrue(params.has("hostname"));
        assertEquals(1, params.getNumberOfParameters());
    }

    @Test
    void fromArgsSupportsEqualsForm() {
        ParameterTool params = ParameterTool.fromArgs(new String[] {"--port=3306"});

        assertEquals("3306", params.get("port"));
    }

    @Test
    void getWithDefaultUsesFallbackWhenAbsent() {
        ParameterTool params = ParameterTool.fromArgs(new String[] {});

        // The inherited implementation reads the defaultData field; a null map would NPE here.
        assertEquals("fallback", params.get("missing", "fallback"));
        assertEquals(5, params.getInt("missing", 5));
        assertEquals(5L, params.getLong("missing", 5L));
        assertTrue(params.getBoolean("missing", true));
        assertFalse(params.getBoolean("flag", false));
    }

    @Test
    void getWithDefaultPrefersPresentValue() {
        ParameterTool params = ParameterTool.fromArgs(new String[] {"--port", "3306"});

        assertEquals("3306", params.get("port", "9999"));
        assertEquals(3306, params.getInt("port", 9999));
    }

    @Test
    void getReturnsNullWhenAbsent() {
        ParameterTool params = ParameterTool.fromArgs(new String[] {});

        assertNull(params.get("missing"));
        assertFalse(params.has("missing"));
    }

    @Test
    void bareFlagUsesNoValueSentinel() {
        ParameterTool params = ParameterTool.fromArgs(new String[] {"--verbose", "--host", "h"});

        // Mirrors Flink: a flag with no following value is stored under __NO_VALUE_KEY.
        assertEquals("__NO_VALUE_KEY", params.get("verbose"));
        assertEquals("h", params.get("host"));
    }

    @Test
    void rejectsArgumentsWithoutDoubleDash() {
        assertThrows(
                IllegalStateException.class,
                () -> ParameterTool.fromArgs(new String[] {"hostname", "localhost"}));
    }

    @Test
    void fromMapRoundTrips() {
        Map<String, String> map = new HashMap<>();
        map.put("a", "1");

        ParameterTool params = ParameterTool.fromMap(map);

        assertEquals("1", params.get("a"));
        assertEquals(map, params.toMap());
    }

    @Test
    void mergeWithPrefersIncomingValues() {
        ParameterTool base = ParameterTool.fromArgs(new String[] {"--a", "1", "--b", "2"});
        ParameterTool override = ParameterTool.fromArgs(new String[] {"--b", "9"});

        ParameterTool merged = base.mergeWith(override);

        assertSame(base, merged);
        assertEquals("1", merged.get("a"));
        assertEquals("9", merged.get("b"));
    }

    @Test
    void unrequestedParametersIsNotInitialisedToNull() {
        // Guards the constructor: Flink initialises this set in a private constructor we cannot
        // call.
        ParameterTool params = ParameterTool.fromArgs(new String[] {"--a", "1"});

        assertNotNull(params.getUnrequestedParameters());
    }

    @Test
    void isUsableAsGlobalJobParameters() {
        // The whole reason this type extends AbstractParameterTool instead of wrapping it.
        ParameterTool params = ParameterTool.fromArgs(new String[] {"--a", "1"});
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        assertNotNull(env.getConfig().getGlobalJobParameters());
    }

    @Test
    void setCheckpointStorageAcceptsAFilePath() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Each line stores this differently: Flink 1.20 calls
        // CheckpointConfig#setCheckpointStorage,
        // while Flink 2.x writes the CheckpointingOptions option. Asserting the one observable both
        // lines share keeps this test version-neutral; each line's own module tests cover the
        // effect.
        FlinkCompat.setCheckpointStorage(env, "file:///tmp/checkpoints");

        assertNotNull(env.getCheckpointConfig());
    }
}
