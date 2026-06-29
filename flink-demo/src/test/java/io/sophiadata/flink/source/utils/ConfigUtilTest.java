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

package io.sophiadata.flink.source.utils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ConfigUtil}. */
class ConfigUtilTest {

    // -------------------------------------------------------------------------
    // loadJsonFile
    // -------------------------------------------------------------------------

    @Test
    void loadJsonFileReadsFromClasspath() {
        // "log4j.properties" is always on the classpath in test scope
        String content = ConfigUtil.loadJsonFile("log4j.properties");
        assertThat(content).isNotNull();
        assertThat(content).isNotEmpty();
    }

    @Test
    void loadJsonFileThrowsForMissingFile() {
        // ConfigUtil wraps IOException in RuntimeException, but if the stream is null
        // the NPE is not caught. The test verifies that some exception is thrown.
        assertThatThrownBy(() -> ConfigUtil.loadJsonFile("this-file-does-not-exist-12345.json"))
                .isInstanceOf(RuntimeException.class);
    }

    // -------------------------------------------------------------------------
    // getJarDir
    // -------------------------------------------------------------------------

    @Test
    void getJarDirReturnsNonNullInTestEnvironment() {
        // In test environment (IDE or maven surefire), this returns a non-null directory
        String jarDir = ConfigUtil.getJarDir();
        assertThat(jarDir).isNotNull();
    }
}
