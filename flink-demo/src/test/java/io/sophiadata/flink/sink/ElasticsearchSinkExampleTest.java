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

class ElasticsearchSinkExampleTest {

    @Test
    void shouldParseDefaultArgs() {
        final String host = getArg(new String[] {}, "es.host", "localhost");
        assertThat(host).isEqualTo("localhost");
    }

    @Test
    void shouldParseCustomArgs() {
        final String[] args = {"--es.host", "10.0.0.1", "--es.port", "9300"};
        final String host = getArg(args, "es.host", "localhost");
        final String port = getArg(args, "es.port", "9200");
        assertThat(host).isEqualTo("10.0.0.1");
        assertThat(port).isEqualTo("9300");
    }

    @Test
    void shouldParseIntArgs() {
        final String[] args = {"--es.port", "9200"};
        final int port = getArgInt(args, "es.port", 9300);
        assertThat(port).isEqualTo(9200);
    }

    @Test
    void shouldReturnDefaultIntWhenMissing() {
        final int port = getArgInt(new String[] {}, "es.port", 9300);
        assertThat(port).isEqualTo(9300);
    }

    private static String getArg(final String[] args, final String key, final String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (("--" + key).equals(args[i])) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }

    private static int getArgInt(final String[] args, final String key, final int defaultValue) {
        final String value = getArg(args, key, String.valueOf(defaultValue));
        return Integer.parseInt(value);
    }
}
