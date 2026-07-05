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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaSourceExampleTest {

    @Test
    void shouldParseDefaultArgs() {
        final String brokers = getArg(new String[] {}, "kafka.brokers", "localhost:9092");
        assertThat(brokers).isEqualTo("localhost:9092");
    }

    @Test
    void shouldParseCustomArgs() {
        final String[] args = {"--kafka.brokers", "10.0.0.1:9092", "--kafka.topic", "my-topic"};
        assertThat(getArg(args, "kafka.brokers", "localhost:9092")).isEqualTo("10.0.0.1:9092");
        assertThat(getArg(args, "kafka.topic", "default")).isEqualTo("my-topic");
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
