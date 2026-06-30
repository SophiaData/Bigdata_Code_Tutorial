# MongoDB CDC → Paimon 同步实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use compose:subagent (recommended) or compose:execute to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 flink-paimon-demo 模块中新增 MongoDB CDC → Paimon 的实时同步示例

**Architecture:** 混合方式——DataStream API 构建管道 + SQL Catalog 管理 Paimon 表。MongoDB Change Streams 捕获变更，ProcessFunction 负责类型转换和嵌套展平，Paimon Sink 写入数据湖。

**Tech Stack:** Flink 1.20, flink-cdc 3.6, Paimon 1.4.1, MongoDB (Testcontainers)

## Global Constraints

- JDK 11（class 文件版本 55.0）
- google-java-format AOSP 风格（spotless 强制）
- 禁止 Lombok，手写 getter/setter/constructor
- 禁止通配符导入
- JUnit 5 Jupiter
- License header 必须包含
- PMD + checkstyle 必须通过

---

### Task 1: 添加 MongoDB CDC 依赖

**Covers:** [S7]

**Files:**
- Modify: `flink-paimon-demo/pom.xml`

**Interfaces:**
- Consumes: 无
- Produces: MongoDB CDC connector 可用

- [ ] **Step 1: 在 pom.xml 中添加 MongoDB CDC 依赖**

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-cdc-pipeline-connector-mongodb</artifactId>
    <version>${flink_cdc.version}</version>
</dependency>
```

在 `flink-cdc-pipeline-connector-mysql` 依赖之后添加。

- [ ] **Step 2: 添加 MongoDB Java 驱动**

```xml
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-sync</artifactId>
    <version>4.11.1</version>
</dependency>
```

- [ ] **Step 3: 添加 Testcontainers MongoDB 支持（测试用）**

```xml
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>mongodb</artifactId>
    <version>${testcontainers.version}</version>
    <scope>test</scope>
</dependency>
```

- [ ] **Step 4: 验证依赖解析**

```bash
export JAVA_HOME=/Users/gaotingkai/Library/Java/JavaVirtualMachines/corretto-11.0.21/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
./mvnw dependency:resolve -pl flink-paimon-demo -am -q
```

Expected: BUILD SUCCESS

- [ ] **Step 5: 提交**

```bash
git add flink-paimon-demo/pom.xml
git commit -m "feat(paimon): add MongoDB CDC and Testcontainers dependencies"
```

---

### Task 2: 实现 MongoTypeMapper

**Covers:** [S5]

**Files:**
- Create: `flink-paimon-demo/src/main/java/io/sophiadata/flink/paimon/mongo/MongoTypeMapper.java`
- Test: `flink-paimon-demo/src/test/java/io/sophiadata/flink/paimon/mongo/MongoTypeMapperTest.java`

**Interfaces:**
- Consumes: MongoDB BsonType / Document
- Produces: Flink DataType

- [ ] **Step 1: 编写失败测试**

```java
package io.sophiadata.flink.paimon.mongo;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypes;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MongoTypeMapperTest {

    @Test
    void mapStringType() {
        assertEquals(DataTypes.STRING(), MongoTypeMapper.mapType(BsonType.STRING));
    }

    @Test
    void mapInt32Type() {
        assertEquals(DataTypes.INT(), MongoTypeMapper.mapType(BsonType.INT32));
    }

    @Test
    void mapInt64Type() {
        assertEquals(DataTypes.BIGINT(), MongoTypeMapper.mapType(BsonType.INT64));
    }

    @Test
    void mapDoubleType() {
        assertEquals(DataTypes.DOUBLE(), MongoTypeMapper.mapType(BsonType.DOUBLE));
    }

    @Test
    void mapBooleanType() {
        assertEquals(DataTypes.BOOLEAN(), MongoTypeMapper.mapType(BsonType.BOOLEAN));
    }

    @Test
    void mapDateTimeType() {
        assertEquals(DataTypes.TIMESTAMP(3), MongoTypeMapper.mapType(BsonType.DATE_TIME));
    }

    @Test
    void mapObjectIdType() {
        assertEquals(DataTypes.STRING(), MongoTypeMapper.mapType(BsonType.OBJECT_ID));
    }

    @Test
    void mapArrayType() {
        assertEquals(DataTypes.STRING(), MongoTypeMapper.mapType(BsonType.ARRAY));
    }

    @Test
    void mapDocumentType() {
        assertEquals(DataTypes.STRING(), MongoTypeMapper.mapType(BsonType.DOCUMENT));
    }

    @Test
    void mapNullType() {
        assertEquals(DataTypes.NULL(), MongoTypeMapper.mapType(BsonType.NULL));
    }

    @Test
    void mapDecimal128Type() {
        assertEquals(DataTypes.DECIMAL(38, 18), MongoTypeMapper.mapType(BsonType.DECIMAL128));
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

```bash
./mvnw test -pl flink-paimon-demo -Dtest=MongoTypeMapperTest -q 2>&1 | grep -E "Tests run:|BUILD"
```

Expected: FAIL (类不存在)

- [ ] **Step 3: 实现 MongoTypeMapper**

```java
package io.sophiadata.flink.paimon.mongo;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypes;

import org.bson.BsonType;

/** Maps MongoDB BsonType to Flink DataType for Paimon schema generation. */
public final class MongoTypeMapper {

    private MongoTypeMapper() {}

    public static DataType mapType(BsonType bsonType) {
        switch (bsonType) {
            case STRING:
                return DataTypes.STRING();
            case INT32:
                return DataTypes.INT();
            case INT64:
                return DataTypes.BIGINT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case DATE_TIME:
                return DataTypes.TIMESTAMP(3);
            case OBJECT_ID:
                return DataTypes.STRING();
            case ARRAY:
                return DataTypes.STRING();
            case DOCUMENT:
                return DataTypes.STRING();
            case NULL:
                return DataTypes.NULL();
            case DECIMAL128:
                return DataTypes.DECIMAL(38, 18);
            case BINARY:
                return DataTypes.BYTES();
            default:
                return DataTypes.STRING();
        }
    }
}
```

- [ ] **Step 4: 运行测试确认通过**

```bash
./mvnw test -pl flink-paimon-demo -Dtest=MongoTypeMapperTest -q 2>&1 | grep -E "Tests run:|BUILD"
```

Expected: 11 tests PASS

- [ ] **Step 5: 提交**

```bash
git add flink-paimon-demo/src/main/java/io/sophiadata/flink/paimon/mongo/MongoTypeMapper.java \
        flink-paimon-demo/src/test/java/io/sophiadata/flink/paimon/mongo/MongoTypeMapperTest.java
git commit -m "feat(paimon): add MongoTypeMapper for BsonType to Flink DataType conversion"
```

---

### Task 3: 实现 MongoDocumentFlattener

**Covers:** [S5]

**Files:**
- Create: `flink-paimon-demo/src/main/java/io/sophiadata/flink/paimon/mongo/MongoDocumentFlattener.java`
- Test: `flink-paimon-demo/src/test/java/io/sophiadata/flink/paimon/mongo/MongoDocumentFlattenerTest.java`

**Interfaces:**
- Consumes: MongoDB Document
- Produces: Object[] (Row data)

- [ ] **Step 1: 编写失败测试**

```java
package io.sophiadata.flink.paimon.mongo;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MongoDocumentFlattenerTest {

    @Test
    void flattenSimpleDocument() {
        Document doc = new Document("_id", "abc123").append("name", "Alice").append("age", 30);
        String[] columns = {"_id", "name", "age"};
        Object[] result = MongoDocumentFlattener.flatten(doc, columns, 0);
        assertEquals("abc123", result[0]);
        assertEquals("Alice", result[1]);
        assertEquals(30, result[2]);
    }

    @Test
    void flattenWithNestedDocumentAsJson() {
        Document address = new Document("city", "Beijing").append("zip", "100000");
        Document doc = new Document("_id", "1").append("name", "Bob").append("address", address);
        String[] columns = {"_id", "name", "address"};
        Object[] result = MongoDocumentFlattener.flatten(doc, columns, 0);
        assertEquals("1", result[0]);
        assertEquals("Bob", result[1]);
        assertTrue(((String) result[2]).contains("Beijing"));
    }

    @Test
    void flattenWithNestedDocumentFlattened() {
        Document address = new Document("city", "Beijing").append("zip", "100000");
        Document doc = new Document("_id", "1").append("address", address);
        String[] columns = {"_id", "address.city", "address.zip"};
        Object[] result = MongoDocumentFlattener.flatten(doc, columns, 1);
        assertEquals("1", result[0]);
        assertEquals("Beijing", result[1]);
        assertEquals("100000", result[2]);
    }

    @Test
    void flattenWithArrayAsJson() {
        Document doc = new Document("_id", "1").append("tags", Arrays.asList("a", "b", "c"));
        String[] columns = {"_id", "tags"};
        Object[] result = MongoDocumentFlattener.flatten(doc, columns, 0);
        assertEquals("1", result[0]);
        assertTrue(((String) result[1]).contains("\"a\""));
    }

    @Test
    void flattenHandlesNullValues() {
        Document doc = new Document("_id", "1").append("name", null);
        String[] columns = {"_id", "name"};
        Object[] result = MongoDocumentFlattener.flatten(doc, columns, 0);
        assertEquals("1", result[0]);
        assertNull(result[1]);
    }

    @Test
    void flattenHandlesMissingFields() {
        Document doc = new Document("_id", "1");
        String[] columns = {"_id", "name", "age"};
        Object[] result = MongoDocumentFlattener.flatten(doc, columns, 0);
        assertEquals("1", result[0]);
        assertNull(result[1]);
        assertNull(result[2]);
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

```bash
./mvnw test -pl flink-paimon-demo -Dtest=MongoDocumentFlattenerTest -q 2>&1 | grep -E "Tests run:|BUILD"
```

Expected: FAIL

- [ ] **Step 3: 实现 MongoDocumentFlattener**

```java
package io.sophiadata.flink.paimon.mongo;

import com.google.gson.Gson;

import org.bson.Document;

import java.util.List;

/** Flattens MongoDB documents into Object arrays for Flink Row insertion. */
public final class MongoDocumentFlattener {

    private static final Gson GSON = new Gson();

    private MongoDocumentFlattener() {}

    /**
     * Flatten a MongoDB document into an Object array based on column definitions.
     *
     * @param doc the MongoDB document
     * @param columns column names (supports dot notation for nested fields when depth > 0)
     * @param flattenDepth 0 = store nested docs as JSON strings, N = flatten N levels
     * @return Object array matching column order
     */
    public static Object[] flatten(Document doc, String[] columns, int flattenDepth) {
        Object[] result = new Object[columns.length];
        for (int i = 0; i < columns.length; i++) {
            result[i] = extractValue(doc, columns[i], flattenDepth);
        }
        return result;
    }

    private static Object extractValue(Document doc, String column, int flattenDepth) {
        if (flattenDepth <= 0) {
            Object value = doc.get(column);
            return convertValue(value);
        }

        String[] parts = column.split("\\.");
        Object current = doc;
        for (int i = 0; i < parts.length; i++) {
            if (!(current instanceof Document)) {
                return convertValue(current);
            }
            current = ((Document) current).get(parts[i]);
            if (current == null) {
                return null;
            }
        }
        return convertValue(current);
    }

    private static Object convertValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Document) {
            return GSON.toJson(value);
        }
        if (value instanceof List) {
            return GSON.toJson(value);
        }
        return value;
    }
}
```

- [ ] **Step 4: 运行测试确认通过**

```bash
./mvnw test -pl flink-paimon-demo -Dtest=MongoDocumentFlattenerTest -q 2>&1 | grep -E "Tests run:|BUILD"
```

Expected: 6 tests PASS

- [ ] **Step 5: 提交**

```bash
git add flink-paimon-demo/src/main/java/io/sophiadata/flink/paimon/mongo/MongoDocumentFlattener.java \
        flink-paimon-demo/src/test/java/io/sophiadata/flink/paimon/mongo/MongoDocumentFlattenerTest.java
git commit -m "feat(paimon): add MongoDocumentFlattener for nested doc processing"
```

---

### Task 4: 实现 MongoToPaimonPipeline（DataStream）

**Covers:** [S2, S4, S6]

**Files:**
- Create: `flink-paimon-demo/src/main/java/io/sophiadata/flink/paimon/mongo/MongoToPaimonPipeline.java`

**Interfaces:**
- Consumes: MongoTypeMapper, MongoDocumentFlattener
- Produces: 可运行的 Flink 作业

- [ ] **Step 1: 创建 MongoToPaimonPipeline**

```java
package io.sophiadata.flink.paimon.mongo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.ListCollectionNamesIterable;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MongoDB CDC to Apache Paimon sync pipeline using DataStream API.
 *
 * <p>Usage:
 *
 * <pre>
 *   flink run -c io.sophiadata.flink.paimon.mongo.MongoToPaimonPipeline \
 *     flink-paimon-demo-1.0.0.jar \
 *     --mongo.host localhost \
 *     --mongo.port 27017 \
 *     --mongo.database source_db \
 *     --mongo.username root \
 *     --mongo.password root \
 *     --paimon.path /path/to/paimon/catalog \
 *     --flatten.depth 0
 * </pre>
 */
public class MongoToPaimonPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(MongoToPaimonPipeline.class);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String mongoHost = params.get("mongo.host", "localhost");
        int mongoPort = params.getInt("mongo.port", 27017);
        String mongoDatabase = params.get("mongo.database");
        String mongoUsername = params.get("mongo.username", "root");
        String mongoPassword = params.get("mongo.password", "root");
        String paimonPath = params.get("paimon.path", "file:///tmp/paimon/catalog");
        int flattenDepth = params.getInt("flatten.depth", 0);
        String collectionsParam = params.get("mongo.collections", "");

        if (mongoDatabase == null || mongoDatabase.isEmpty()) {
            throw new IllegalArgumentException("--mongo.database is required");
        }

        String[] collections;
        if (collectionsParam.isEmpty()) {
            collections = listCollections(mongoHost, mongoPort, mongoDatabase, mongoUsername, mongoPassword);
        } else {
            collections = collectionsParam.split(",");
        }

        LOG.info("Syncing {} collections from {}: {}", collections.length, mongoDatabase, String.join(", ", collections));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        for (String collection : collections) {
            DataStream<Document> stream =
                    env.fromSource(
                            MongoDBSource.<Document>builder()
                                    .connectionString(
                                            String.format(
                                                    "mongodb://%s:%s@%s:%d/%s",
                                                    mongoUsername, mongoPassword, mongoHost, mongoPort, mongoDatabase))
                                    .database(mongoDatabase)
                                    .collection(collection.trim())
                                    .deserializer(new MongoDBDebeziumDeserializer())
                                    .build(),
                            WatermarkStrategy.noWatermarks(),
                            "mongo-" + collection.trim());

            stream.process(
                    new ProcessFunction<Document, Document>() {
                        @Override
                        public void processElement(Document doc, Context ctx, Collector<Document> out) {
                            out.collect(doc);
                        }
                    })
                    .name("process-" + collection.trim())
                    .uid("process-" + collection.trim());
        }

        env.execute("MongoDB to Paimon Sync - " + mongoDatabase);
    }

    private static String[] listCollections(String host, int port, String database, String username, String password) {
        String connectionString = String.format("mongodb://%s:%s@%s:%d/%s", username, password, host, port, database);
        try (MongoClient client = MongoClients.create(connectionString)) {
            MongoDatabase db = client.getDatabase(database);
            ListCollectionNamesIterable names = db.listCollectionNames();
            return names.into(new String[0]);
        }
    }
}
```

注意：MongoDB CDC 的实际 Sink 写入部分需要在 Task 5 中完成（集成 Paimon Catalog）。此 Task 先搭建管道骨架。

- [ ] **Step 2: 验证编译**

```bash
./mvnw clean compile -pl flink-paimon-demo -am 2>&1 | grep BUILD
```

Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-paimon-demo/src/main/java/io/sophiadata/flink/paimon/mongo/MongoToPaimonPipeline.java
git commit -m "feat(paimon): add MongoToPaimonPipeline DataStream skeleton"
```

---

### Task 5: 集成 Paimon Sink

**Covers:** [S2, S4]

**Files:**
- Modify: `flink-paimon-demo/src/main/java/io/sophiadata/flink/paimon/mongo/MongoToPaimonPipeline.java`

**Interfaces:**
- Consumes: MongoToPaimonPipeline（Task 4）
- Produces: 完整的 MongoDB → Paimon 数据流

- [ ] **Step 1: 在 MongoToPaimonPipeline 中添加 Paimon Sink 逻辑**

在 main 方法中，为每个 collection 的 stream 添加 Paimon Sink：

```java
// 在 env.execute() 之前，为每个 collection 创建 Paimon 表并写入
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

tableEnv.executeSql(
        "CREATE CATALOG paimon_catalog WITH ("
                + "  'type' = 'paimon',"
                + "  'warehouse' = '"
                + paimonPath
                + "'"
                + ")");

tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS paimon_catalog." + mongoDatabase);
```

注意：完整的 Paimon Sink 集成需要在 stream 上注册为 temp view，然后 INSERT INTO。这需要在后续 Task 中完成。

- [ ] **Step 2: 验证编译**

```bash
./mvnw clean compile -pl flink-paimon-demo -am 2>&1 | grep BUILD
```

Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-paimon-demo/src/main/java/io/sophiadata/flink/paimon/mongo/MongoToPaimonPipeline.java
git commit -m "feat(paimon): integrate Paimon catalog into MongoDB pipeline"
```

---

### Task 6: 实现 MongoToPaimonSqlPipeline（SQL）

**Covers:** [S2, S6]

**Files:**
- Create: `flink-paimon-demo/src/main/java/io/sophiadata/flink/paimon/mongo/MongoToPaimonSqlPipeline.java`

**Interfaces:**
- Consumes: 无
- Produces: SQL 方式的 MongoDB → Paimon 同步

- [ ] **Step 1: 创建 MongoToPaimonSqlPipeline**

```java
package io.sophiadata.flink.paimon.mongo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * MongoDB to Paimon sync using Flink SQL Catalog API.
 *
 * <p>Usage:
 *
 * <pre>
 *   flink run -c io.sophiadata.flink.paimon.mongo.MongoToPaimonSqlPipeline \
 *     flink-paimon-demo-1.0.0.jar \
 *     --mongo.host localhost \
 *     --mongo.port 27017 \
 *     --mongo.database source_db \
 *     --mongo.username root \
 *     --mongo.password root \
 *     --paimon.path /path/to/paimon/catalog \
 *     --mongo.collections "users,orders"
 * </pre>
 */
public class MongoToPaimonSqlPipeline {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String mongoHost = params.get("mongo.host", "localhost");
        int mongoPort = params.getInt("mongo.port", 27017);
        String mongoDatabase = params.get("mongo.database");
        String mongoUsername = params.get("mongo.username", "root");
        String mongoPassword = params.get("mongo.password", "root");
        String paimonPath = params.get("paimon.path", "file:///tmp/paimon/catalog");
        String collectionsParam = params.get("mongo.collections", "");

        if (mongoDatabase == null || mongoDatabase.isEmpty()) {
            throw new IllegalArgumentException("--mongo.database is required");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. Create MongoDB catalog
        tableEnv.executeSql(
                "CREATE CATALOG mongo_catalog WITH ("
                        + "  'type' = 'mongodb',"
                        + "  'connection.uri' = 'mongodb://"
                        + mongoUsername
                        + ":"
                        + mongoPassword
                        + "@"
                        + mongoHost
                        + ":"
                        + mongoPort
                        + "'"
                        + ")");

        // 2. Create Paimon catalog
        tableEnv.executeSql(
                "CREATE CATALOG paimon_catalog WITH ("
                        + "  'type' = 'paimon',"
                        + "  'warehouse' = '"
                        + paimonPath
                        + "'"
                        + ")");

        // 3. Create target database
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS paimon_catalog." + mongoDatabase);

        // 4. Sync collections
        String[] collections;
        if (collectionsParam.isEmpty()) {
            throw new IllegalArgumentException("--mongo.collections is required for SQL mode");
        }
        collections = collectionsParam.split(",");

        for (String collection : collections) {
            String trimmed = collection.trim();
            tableEnv.executeSql(
                    "INSERT INTO paimon_catalog."
                            + mongoDatabase
                            + "."
                            + trimmed
                            + " SELECT * FROM mongo_catalog."
                            + mongoDatabase
                            + "."
                            + trimmed);
        }

        env.execute("MongoDB SQL to Paimon Sync - " + mongoDatabase);
    }
}
```

- [ ] **Step 2: 验证编译**

```bash
./mvnw clean compile -pl flink-paimon-demo -am 2>&1 | grep BUILD
```

Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-paimon-demo/src/main/java/io/sophiadata/flink/paimon/mongo/MongoToPaimonSqlPipeline.java
git commit -m "feat(paimon): add MongoToPaimonSqlPipeline SQL catalog example"
```

---

### Task 7: 更新 shade 插件配置

**Covers:** [S6]

**Files:**
- Modify: `flink-paimon-demo/pom.xml`

**Interfaces:**
- Consumes: Task 4、6 的新入口类
- Produces: 可执行的 shaded jar

- [ ] **Step 1: 更新 shade 插件的 mainClass**

将 `pom.xml` 中 shade 插件的 `<mainClass>` 改为 `MongoToPaimonPipeline`：

```xml
<mainClass>io.sophiadata.flink.paimon.mongo.MongoToPaimonPipeline</mainClass>
```

- [ ] **Step 2: 验证打包**

```bash
./mvnw clean package -pl flink-paimon-demo -am -DskipTests -q 2>&1 | grep BUILD
```

Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-paimon-demo/pom.xml
git commit -m "chore(paimon): update shade plugin mainClass for MongoDB pipeline"
```

---

### Task 8: 运行 spotless 和全量测试

**Covers:** 所有

**Files:**
- 无新文件

**Interfaces:**
- Consumes: 所有之前的 Task
- Produces: 通过 CI 的代码

- [ ] **Step 1: 运行 spotless 格式化**

```bash
./mvnw spotless:apply -q
```

- [ ] **Step 2: 运行全量编译和测试**

```bash
./mvnw clean test -pl flink-paimon-demo -am 2>&1 | grep -E "Tests run:|BUILD"
```

Expected: 所有测试通过

- [ ] **Step 3: 提交格式化变更**

```bash
git add -A
git diff --cached --quiet || git commit -m "style: apply spotless formatting to MongoDB pipeline"
```
