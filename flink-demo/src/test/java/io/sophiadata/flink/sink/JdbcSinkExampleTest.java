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

package io.sophiadata.flink.sink;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcSinkExampleTest {

    @Test
    void shouldParseDefaultArgs() {
        assertThat(getArg(new String[] {}, "jdbc.url", "jdbc:mysql://localhost:3306/test"))
                .isEqualTo("jdbc:mysql://localhost:3306/test");
    }

    @Test
    void shouldParseCustomArgs() {
        final String[] args = {"--jdbc.url", "jdbc:postgresql://localhost:5432/test"};
        assertThat(getArg(args, "jdbc.url", "default"))
                .isEqualTo("jdbc:postgresql://localhost:5432/test");
    }

    private static String getArg(final String[] args, final String key, final String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (("--" + key).equals(args[i])) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
}
