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

import io.sophiadata.flink.compat.ParameterTool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NacosUtilTest {

    @Test
    void mergeInto_isNoOpWhenNoNacosServerKey() {
        ParameterTool args = ParameterTool.fromArgs(new String[] {"--hostname", "localhost"});
        ParameterTool merged = NacosUtil.mergeInto(args);
        // Same instance returned (no Nacos fetch attempted).
        assertSame(args, merged);
        assertEquals("localhost", merged.get("hostname"));
    }

    @Test
    void mergeInto_failsFastWhenNacosServerUnreachable() {
        ParameterTool args =
                ParameterTool.fromArgs(
                        new String[] {
                            "--nacos_server", "127.0.0.1:1", // closed port
                            "--nacos_username", "nacos",
                            "--nacos_pd", "nacos",
                            "--hostname", "localhost"
                        });
        // Should throw IllegalStateException wrapping the underlying NacosException /
        // IOException rather than silently returning args.
        assertThrows(IllegalStateException.class, () -> NacosUtil.mergeInto(args));
    }

    @Test
    void getFromNacosConfig_rejectsMissingServerAddr() {
        // Empty nacos_server -> Nacos client throws; we surface that as IOException /
        // NacosException, not as a silent success.
        ParameterTool args = ParameterTool.fromArgs(new String[] {});
        assertThrows(
                Exception.class, () -> NacosUtil.getFromNacosConfig("demo", args, "DEFAULT_GROUP"));
    }

    // --- local config file merging ---
    //
    // Only the failure paths were covered. The merge semantics are the part that matters: a config
    // file supplies defaults, and the command line must still win on conflict.

    @Test
    void mergeLocalFile_loadsValuesFromAPropertiesFile(@TempDir Path dir) throws Exception {
        Path config = dir.resolve("config.properties");
        Files.write(
                config, Arrays.asList("hostname=from-file", "port=3306"), StandardCharsets.UTF_8);

        ParameterTool merged =
                NacosUtil.mergeInto(
                        ParameterTool.fromArgs(
                                new String[] {"--config", config.toAbsolutePath().toString()}));

        assertEquals("from-file", merged.get("hostname"));
        assertEquals("3306", merged.get("port"));
    }

    @Test
    void mergeLocalFile_commandLineWinsOnConflict(@TempDir Path dir) throws Exception {
        Path config = dir.resolve("config.properties");
        Files.write(
                config, Arrays.asList("hostname=from-file", "port=3306"), StandardCharsets.UTF_8);

        ParameterTool merged =
                NacosUtil.mergeInto(
                        ParameterTool.fromArgs(
                                new String[] {
                                    "--config",
                                    config.toAbsolutePath().toString(),
                                    "--hostname",
                                    "from-cli"
                                }));

        assertEquals("from-cli", merged.get("hostname"), "the CLI value should take precedence");
        assertEquals("3306", merged.get("port"), "keys only in the file should still apply");
    }

    @Test
    void mergeLocalFile_rejectsMissingFile() {
        ParameterTool args =
                ParameterTool.fromArgs(new String[] {"--config", "/nonexistent/config.properties"});

        assertThrows(IllegalStateException.class, () -> NacosUtil.mergeInto(args));
    }

    @Test
    void mergeLocalFile_rejectsClasspathResourceThatDoesNotExist() {
        ParameterTool args =
                ParameterTool.fromArgs(
                        new String[] {"--config", "classpath:no-such-file.properties"});

        assertThrows(IllegalStateException.class, () -> NacosUtil.mergeInto(args));
    }

    @Test
    void mergeLocalFile_handlesEmptyPropertiesFile(@TempDir Path dir) throws Exception {
        Path config = dir.resolve("empty.properties");
        Files.write(config, Collections.emptyList(), StandardCharsets.UTF_8);

        ParameterTool merged =
                NacosUtil.mergeInto(
                        ParameterTool.fromArgs(
                                new String[] {
                                    "--config",
                                    config.toAbsolutePath().toString(),
                                    "--hostname",
                                    "from-cli"
                                }));

        assertEquals("from-cli", merged.get("hostname"), "CLI values must survive an empty file");
    }

    @Test
    void mergeLocalFile_keepsValuesContainingEquals(@TempDir Path dir) throws Exception {
        // Properties splits on the first separator only, so a JDBC URL must survive intact rather
        // than being truncated at its first '=' or '&'.
        Path config = dir.resolve("config.properties");
        Files.write(
                config,
                Collections.singletonList("sink_url=jdbc:mysql://host:3306/db?useSSL=false&x=1"),
                StandardCharsets.UTF_8);

        ParameterTool merged =
                NacosUtil.mergeInto(
                        ParameterTool.fromArgs(
                                new String[] {"--config", config.toAbsolutePath().toString()}));

        assertEquals("jdbc:mysql://host:3306/db?useSSL=false&x=1", merged.get("sink_url"));
    }
}
