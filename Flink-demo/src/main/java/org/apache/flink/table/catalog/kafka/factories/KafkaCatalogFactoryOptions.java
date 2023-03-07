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

package org.apache.flink.table.catalog.kafka.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** (@SophiaData) (@date 2023/3/6 09:46). */
public class KafkaCatalogFactoryOptions {
    public static final String IDENTIFIER = "kafka";

    public static final String KAFKA_PREFIX = "properties.";
    public static final String SCHEMA_REGISTRY_PREFIX = "schema.registry.";
    public static final String SCAN_PREFIX = "scan.";
    public static final String SINK_PREFIX = "sink.";

    public static final ConfigOption<String> BOOTSTRAP_SERVERS =
            ConfigOptions.key("properties.bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required Bootstrap Servers");

    public static final ConfigOption<String> SCHEMA_REGISTRY_URI =
            ConfigOptions.key("schema.registry.uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required Schema Registry URI");
}
