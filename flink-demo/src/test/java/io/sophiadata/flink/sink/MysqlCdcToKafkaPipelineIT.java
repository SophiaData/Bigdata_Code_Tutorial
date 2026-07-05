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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** 集成测试：验证 MysqlCdcToKafkaPipeline 的反序列化器。 */
public class MysqlCdcToKafkaPipelineIT {

    @Test
    public void testDeserializerHandlesCreateEvent() throws Exception {
        final MysqlCdcToKafkaPipeline.SimpleStringDebeziumDeserializer deserializer =
                new MysqlCdcToKafkaPipeline.SimpleStringDebeziumDeserializer();

        final SourceRecord record =
                new SourceRecord(
                        null,
                        null,
                        "mydb.users",
                        0,
                        null,
                        "key_1",
                        null,
                        "{\"after\":{\"id\":1,\"name\":\"Alice\",\"email\":\"a@b.com\"},\"op\":\"c\"}",
                        null,
                        null);

        final List<String> results = new ArrayList<>();
        deserializer.deserialize(record, new CollectingCollector(results));

        assertThat(results).hasSize(1);
        assertThat(results.get(0)).contains("mydb.users");
        assertThat(results.get(0)).contains("key_1");
        assertThat(results.get(0)).contains("after");
    }

    @Test
    public void testDeserializerSkipsNullValue() throws Exception {
        final MysqlCdcToKafkaPipeline.SimpleStringDebeziumDeserializer deserializer =
                new MysqlCdcToKafkaPipeline.SimpleStringDebeziumDeserializer();

        final SourceRecord record =
                new SourceRecord(null, null, "topic", 0, null, "k", null, null, null, null);

        final List<String> results = new ArrayList<>();
        deserializer.deserialize(record, new CollectingCollector(results));

        assertThat(results).isEmpty();
    }

    @Test
    public void testDeserializerReturnsCorrectType() {
        final MysqlCdcToKafkaPipeline.SimpleStringDebeziumDeserializer deserializer =
                new MysqlCdcToKafkaPipeline.SimpleStringDebeziumDeserializer();

        final TypeInformation<String> type = deserializer.getProducedType();
        assertThat(type).isNotNull();
        assertThat(type.getTypeClass()).isEqualTo(String.class);
    }

    private static class CollectingCollector implements Collector<String> {
        private final List<String> results;

        CollectingCollector(final List<String> results) {
            this.results = results;
        }

        @Override
        public void collect(final String value) {
            results.add(value);
        }

        @Override
        public void close() {}
    }
}
