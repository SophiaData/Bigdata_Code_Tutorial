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

package io.sophiadata.flink.function;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SplitFunctionTest {

    @Test
    void testSplitOnComma() throws Exception {
        List<Row> out = invoke("a,b,c", ",");
        assertEquals(3, out.size());
        assertEquals("a", out.get(0).getField(0));
        assertEquals("b", out.get(1).getField(0));
        assertEquals("c", out.get(2).getField(0));
    }

    @Test
    void testSplitOnDash() throws Exception {
        List<Row> out = invoke("hello-world", "-");
        assertEquals(2, out.size());
        assertEquals("hello", out.get(0).getField(0));
        assertEquals("world", out.get(1).getField(0));
    }

    @Test
    void testNullInputEmitsEmptyToken() throws Exception {
        List<Row> out = invoke(null, ",");
        assertEquals(1, out.size());
        assertEquals("", out.get(0).getField(0));
    }

    private List<Row> invoke(String input, String separator) throws Exception {
        SplitFunction fn = new SplitFunction();
        // TableFunction 通过反射访问 protected collector 字段
        Field collectorField = TableFunction.class.getDeclaredField("collector");
        collectorField.setAccessible(true);
        List<Row> sink = new ArrayList<>();
        Collector<Row> collector = new ListCollector<Row>(sink);
        collectorField.set(fn, collector);

        fn.eval(input, separator);
        return sink;
    }
}
