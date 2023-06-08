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

package org.apache.flink.table.catalog.kafka;

import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.kafka.factories.KafkaAdminClientFactory;
import org.apache.flink.table.catalog.kafka.factories.KafkaCatalogFactoryOptions;
import org.apache.flink.table.catalog.kafka.factories.SchemaRegistryClientFactory;
import org.apache.flink.table.catalog.kafka.json.JsonSchemaConverter;
import org.apache.flink.table.catalog.kafka.schema.TopicSchema;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.DataType;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** (@SophiaData) (@date 2023/3/6 09:54). */
public class KafkaCatalog extends AbstractCatalog {

    public static final String DEFAULT_DB = "default";
    public static final int DEFAULT_CACHE_SIZE = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCatalog.class);
    private final Map<String, String> properties;

    private SchemaRegistryClient schemaRegistryClient;
    private List<String> topics;

    private final KafkaAdminClientFactory kafkaAdminClientFactory;

    public KafkaCatalog(
            String name,
            Map<String, String> properties,
            KafkaAdminClientFactory kafkaAdminClientFactory) {
        this(name, DEFAULT_DB, properties, kafkaAdminClientFactory);
    }

    public KafkaCatalog(
            String name,
            String defaultDatabase,
            Map<String, String> properties,
            KafkaAdminClientFactory kafkaAdminClientFactory) {
        super(name, defaultDatabase);
        this.properties = properties;
        this.kafkaAdminClientFactory = kafkaAdminClientFactory;

        LOG.info("Created Kafka Catalog {}", name);
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new KafkaDynamicTableFactory());
    }

    @Override
    public void open() throws CatalogException {

        Map<String, String> schemaRegistryProperties =
                properties.entrySet().stream()
                        .filter(
                                p ->
                                        p.getKey()
                                                .startsWith(
                                                        KafkaCatalogFactoryOptions
                                                                .SCHEMA_REGISTRY_PREFIX))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        List<String> baseURLs =
                Arrays.asList(
                        schemaRegistryProperties
                                .get(KafkaCatalogFactoryOptions.SCHEMA_REGISTRY_URI.key())
                                .split(","));
        SchemaRegistryClientFactory sf = new SchemaRegistryClientFactory();
        schemaRegistryClient = sf.get(baseURLs, DEFAULT_CACHE_SIZE, schemaRegistryProperties);

        Map<String, Object> kafkaProperties =
                properties.entrySet().stream()
                        .filter(p -> p.getKey().startsWith(KafkaCatalogFactoryOptions.KAFKA_PREFIX))
                        .collect(
                                Collectors.toMap(
                                        p ->
                                                p.getKey()
                                                        .substring(
                                                                KafkaCatalogFactoryOptions
                                                                        .KAFKA_PREFIX
                                                                        .length()),
                                        Map.Entry::getValue));
        try (AdminClient client = kafkaAdminClientFactory.get(kafkaProperties)) {
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(true); // includes internal topics such as __consumer_offsets
            ListTopicsResult topicsResult = client.listTopics(options);
            topics = new ArrayList<>(topicsResult.names().get());
        } catch (Exception e) {
            throw new CatalogException("Could not list topics", e);
        }
    }

    @Override
    public void close() throws CatalogException {}

    @Override
    public List<String> listDatabases() throws CatalogException {
        return Collections.singletonList(getDefaultDatabase());
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (databaseName.equals(getDefaultDatabase())) {
            return new CatalogDatabaseImpl(new HashMap<>(), "");
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return listDatabases().contains(databaseName);
    }

    @Override
    public void createDatabase(
            String databaseName, CatalogDatabase database, boolean ignoreIfExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public void dropDatabase(String s, boolean b, boolean b1) throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public void alterDatabase(String s, CatalogDatabase catalogDatabase, boolean b)
            throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
        if (!this.databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        } else {
            return topics;
        }
    }

    @Override
    public List<String> listViews(String databaseName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        String topic = tablePath.getObjectName();

        try {
            TopicSchema schema = getTopicSchema(topic);
            return CatalogTable.of(
                    schema.getSchema(),
                    "",
                    new ArrayList<>(),
                    getTableProperties(topic, schema.getSchemaType()));
        } catch (Exception e) {
            LOG.error(
                    "Error while accessing table "
                            + tablePath
                            + " : "
                            + ExceptionUtils.getStackTrace(e));
            throw new TableNotExistException(this.getName(), tablePath);
        }
    }

    private TopicSchema getTopicSchema(String topic) throws RestClientException, IOException {
        try {
            SchemaMetadata latestSchemaMetadata =
                    schemaRegistryClient.getLatestSchemaMetadata(topic);
            if (latestSchemaMetadata == null) {
                LOG.warn("Schema does not exist in registry.Instead will use default schema");
                return new TopicSchema(getDefaultSchema(), TopicSchema.SchemaType.RAW);
            }
            return new TopicSchema(
                    getTableSchema(latestSchemaMetadata), latestSchemaMetadata.getSchemaType());
        } catch (Exception e) {
            LOG.error(
                    "Error while preparing schema for "
                            + topic
                            + " : "
                            + ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }

    private Schema getDefaultSchema() {
        DataTypes.Field[] fields = new DataTypes.Field[1];
        fields[0] = DataTypes.FIELD("value", DataTypes.STRING());
        return Schema.newBuilder().fromRowDataType(DataTypes.ROW(fields).notNull()).build();
    }

    private Schema getTableSchema(SchemaMetadata schemaMetaData) {
        DataType dataType;
        switch (Enum.valueOf(TopicSchema.SchemaType.class, schemaMetaData.getSchemaType())) {
            case JSON:
                Optional<ParsedSchema> parsedSchema =
                        schemaRegistryClient.parseSchema(
                                schemaMetaData.getSchemaType(),
                                schemaMetaData.getSchema(),
                                schemaMetaData.getReferences());
                if (!parsedSchema.isPresent()) {
                    throw new IllegalArgumentException("Could not parse Avro schema string.");
                }
                dataType = JsonSchemaConverter.convertToDataType((JsonSchema) parsedSchema.get());
                break;
            case AVRO:
                dataType = AvroSchemaConverter.convertToDataType(schemaMetaData.getSchema());
                break;
            default:
                throw new NotImplementedException("Not supporting serialization format");
        }

        return Schema.newBuilder().fromRowDataType(dataType).build();
    }

    protected Map<String, String> getTableProperties(String topic, TopicSchema.SchemaType type) {
        Map<String, String> props = new HashMap<>();
        props.put("connector", "kafka");
        props.put("topic", topic);
        props.put("scan.startup.mode", "latest-offset");
        props.put("format", type.getFormat());

        properties.entrySet().stream()
                .filter(
                        p ->
                                p.getKey().startsWith(KafkaCatalogFactoryOptions.SCAN_PREFIX)
                                        || p.getKey()
                                                .startsWith(KafkaCatalogFactoryOptions.SINK_PREFIX))
                .forEach(p -> props.put(p.getKey(), p.getValue()));

        properties.entrySet().stream()
                .filter(p -> p.getKey().startsWith(KafkaCatalogFactoryOptions.KAFKA_PREFIX))
                .forEach(p -> props.put(p.getKey(), p.getValue()));

        return props;
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        // TODO: Schema Naming Strategy
        checkNotNull(tablePath, "tablePath cannot be null");
        try {
            String topic = tablePath.getObjectName();
            return topics.contains(topic);
        } catch (Exception e) {
            throw new CatalogException("Could not list table", e);
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newCatalogTable, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public List<String> listFunctions(String databaseName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics tableStatistics,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath objectPath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Kafka only supports read operations");
    }
}
