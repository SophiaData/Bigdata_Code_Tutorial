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

import org.apache.flink.api.java.utils.ParameterTool;

import org.junit.jupiter.api.Test;

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
}
