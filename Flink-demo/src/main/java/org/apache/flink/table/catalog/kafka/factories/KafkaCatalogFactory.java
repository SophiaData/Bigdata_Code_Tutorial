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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.kafka.KafkaCatalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

/** (@SophiaData) (@date 2023/3/6 09:44). */
public class KafkaCatalogFactory implements CatalogFactory {

    public KafkaCatalogFactory() {}

    @Override
    public String factoryIdentifier() {
        return KafkaCatalogFactoryOptions.IDENTIFIER;
    }

    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(KafkaCatalogFactoryOptions.BOOTSTRAP_SERVERS);
        options.add(KafkaCatalogFactoryOptions.SCHEMA_REGISTRY_URI);
        return options;
    }

    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept(
                KafkaCatalogFactoryOptions.KAFKA_PREFIX,
                KafkaCatalogFactoryOptions.SCHEMA_REGISTRY_PREFIX,
                KafkaCatalogFactoryOptions.SCAN_PREFIX,
                KafkaCatalogFactoryOptions.SINK_PREFIX);

        return new KafkaCatalog(
                context.getName(),
                ((Configuration) helper.getOptions()).toMap(),
                new KafkaAdminClientFactory());
    }
}
