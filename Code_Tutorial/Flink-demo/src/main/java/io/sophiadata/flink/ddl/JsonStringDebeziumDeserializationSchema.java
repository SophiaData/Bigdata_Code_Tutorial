/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sophiadata.flink.ddl;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** (@SophiaData) (@date 2022/11/13 11:05). */
public class JsonStringDebeziumDeserializationSchema
        implements DebeziumDeserializationSchema<Tuple2<Boolean, String>> {

    @Override
    public void deserialize(SourceRecord record, Collector<Tuple2<Boolean, String>> out)
            throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        //        Schema valueSchema = record.valueSchema(); // 有需要时打开
        if (op == Envelope.Operation.CREATE) {
            String insert = extractAfterRow(value);
            out.collect(new Tuple2<>(true, insert));
        } else if (op == Envelope.Operation.DELETE) {
            String delete = extractBeforeRow(value);
            out.collect(new Tuple2<>(false, delete));
        }
        // read 操作单独抽离，本地测试发现如不单独操作，getRowMap 方法在 initial 启动模式下报空指针异常
        // 原因可能是 schema change ---- 或者是其他原因
        else if (op == Envelope.Operation.READ) {
            Struct after = value.getStruct("after");
            JSONObject afterJson = new JSONObject();
            if (after != null) {
                List<Field> fields = after.schema().fields();
                for (Field field : fields) {
                    afterJson.put(field.name(), after.get(field));
                    out.collect(new Tuple2<>(true, afterJson.toJSONString()));
                }
            }
        } else {
            String after = extractAfterRow(value);
            out.collect(new Tuple2<>(true, after));
        }
    }

    private Map<String, Object> getRowMap(Struct after) {
        return after.schema().fields().stream().collect(Collectors.toMap(Field::name, after::get));
    }

    private String extractAfterRow(Struct value) throws Exception {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Map<String, Object> rowMap = getRowMap(after);
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(rowMap);
    }

    private String extractBeforeRow(Struct value) throws Exception {
        Struct after = value.getStruct(Envelope.FieldName.BEFORE);
        Map<String, Object> rowMap = getRowMap(after);
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(rowMap);
    }

    @Override
    public TypeInformation<Tuple2<Boolean, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<Boolean, String>>() {});
    }
}
