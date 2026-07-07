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

package io.sophiadata.flink.streaming.advanced;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessFunctionAdvancedExampleTest {

    @Test
    void shouldCreateOrderTimeoutMonitor() {
        final ProcessFunctionAdvancedExample.OrderTimeoutMonitor monitor =
                new ProcessFunctionAdvancedExample.OrderTimeoutMonitor();
        assertThat(monitor).isNotNull();
    }

    @Test
    void shouldHaveSerialVersionUID() throws Exception {
        final java.lang.reflect.Field field =
                ProcessFunctionAdvancedExample.OrderTimeoutMonitor.class.getDeclaredField(
                        "serialVersionUID");
        assertThat(field).isNotNull();
    }
}
