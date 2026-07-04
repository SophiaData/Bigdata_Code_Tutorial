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

class MysqlCdcToKafkaSqlPipelineTest {

    @Test
    void shouldParseDefaultArgs() {
        // 测试默认参数解析逻辑
        final String hostname = getArg(new String[] {}, "mysql.hostname", "localhost");
        assertThat(hostname).isEqualTo("localhost");
    }

    @Test
    void shouldParseCustomArgs() {
        // 测试自定义参数解析
        final String[] args = {"--mysql.hostname", "10.0.0.1", "--mysql.port", "3307"};
        final String hostname = getArg(args, "mysql.hostname", "localhost");
        final String port = getArg(args, "mysql.port", "3306");
        assertThat(hostname).isEqualTo("10.0.0.1");
        assertThat(port).isEqualTo("3307");
    }

    @Test
    void shouldHandleMissingArg() {
        // 测试缺失参数时返回默认值
        final String topic = getArg(new String[] {}, "kafka.topic", "default-topic");
        assertThat(topic).isEqualTo("default-topic");
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
