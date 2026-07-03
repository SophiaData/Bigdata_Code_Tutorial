# 测试策略

> 适用于 `Bigdata_Code_Tutorial` 所有模块

---

## 一、测试分层

```
┌─────────────────────────────────────────┐
│         End-to-End (FlinkSqlWDSTest)    │  ← 需要真实 MySQL × 2
├─────────────────────────────────────────┤
│      Integration Tests (*IT)             │  ← testcontainers MySQL
├─────────────────────────────────────────┤
│         Unit Tests                      │  ← mock / faker / assertj
└─────────────────────────────────────────┘
```

### 1.1 各层定义

| 层级 | 命名约定 | 运行场景 | 工具 |
|---|---|---|---|
| **单元测试** | `*Test.java` | 本地开发、CI Job 1 | JUnit 5 + Mockito + AssertJ + JavaFaker |
| **集成测试** | `*IT.java` | CI Job 2（testcontainers） | JUnit 5 + testcontainers MySQL |
| **端到端测试** | `FlinkSqlWDSTest.java` | CI Job 2（需 secrets） | JUnit 5 + 真实 MySQL × 2 |

---

## 二、单元测试规范

### 2.1 何时写单元测试

**必须覆盖的场景**：
- 工具类：`ConfigUtil`、`ParamUtil`、`RandomOptionGroup`、`PropertiesUtil`
- 数据转换逻辑：`CdcEventDeserializer`、`Record` 内部类
- SchemaEvolver 中的字段映射逻辑

**允许不写测试的场景**：
- 示例代码（`src/main/java/.../example/`）
- 直接调用 Flink API 的胶水代码（难以 mock）

### 2.2 依赖使用约定

| 场景 | 工具 | 说明 |
|---|---|---|
| 断言 | `assertj-core` | 链式 API，比 JUnit assert 更可读 |
| Mock 对象 | `mockito-core` | 隔离外部依赖 |
| 假数据 | `javafaker` | 生成姓名/地址/UUID 等真实感数据 |
| Mock Flink CDC 事件 | 手动构造 | 直接 new `DataChangeEvent`、`CreateTableEvent` 等 |

### 2.3 示例：Mock CDC 事件

```java
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.sophiadata.flink.cdc.MySqlSourceExampleTest;
import java.util.Collections;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.junit.jupiter.api.Test;

class CdcEventDeserializerTest {

    @Test
    void shouldExtractColumnsFromCreateTableEvent() {
        // given: a CDC CreateTableEvent with known columns
        final Schema schema = Schema.newBuilder()
                .column("id", io.sophiadata.flink.cdc.common.schema.Schema.ColumnDataType.BIGINT)
                .column("name", io.sophiadata.flink.cdc.common.schema.Schema.ColumnDataType.STRING)
                .primaryKey("id")
                .build();
        final CreateTableEvent event = new CreateTableEvent(
                io.sophiadata.flink.cdc.common.event.TableId.tableId("mydb", "t_user"),
                schema);

        // when: deserialize extracts column metadata
        final var columns = extractColumnNames(event);

        // then: primary key first, then others
        assertThat(columns).containsExactly("id", "name");
    }
}
```

### 2.4 示例：使用 JavaFaker 生成测试数据

```java
import com.github.javafaker.Faker;

class MysqlUtilTest {

    private final Faker faker = new Faker();

    @Test
    void shouldConnectWithValidCredentials() {
        // given: use faker to generate a unique suffix for test isolation
        final String testDb = "test_" + faker.number().randomNumber(6, false);

        // when/then
        assertThatCode(() -> DriverManager.getConnection(
                        "jdbc:mysql://localhost:3306/" + testDb + "?useSSL=false",
                        "root", "root"))
                .doesNotThrowAnyException();
    }
}
```

---

## 三、集成测试规范（testcontainers）

### 3.1 MySQL 版本固定

`MySqlVersion` 枚举固定为 `8.0.36`：

```java
// ✅ 正确：固定版本
new MySqlContainer(MySqlVersion.V8_0)

// ❌ 错误：latest 可能随时间漂移
new MySqlContainer(MySqlVersion.V8_LATEST)
```

### 3.2 容器生命周期管理

使用 `@Container` 和 `@Testcontainers` 注解自动管理容器：

```java
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class CreateMysqlLSinkTableIT {

    @Container
    static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>("mysql:8.0.36")
                    .withDatabaseName("testdb")
                    .withUsername("root")
                    .withPassword("root");
    // 容器在同一测试类所有方法间复用，自动启动/停止

    @Test
    void shouldCreateSinkTable() {
        final String jdbcUrl = MYSQL.getJdbcUrl();
        // use jdbcUrl to test DDL execution
    }
}
```

### 3.3 测试隔离

每个集成测试使用**独立的数据库**，避免并发冲突：

```java
// 使用 UniqueDatabase 工具类为每个测试生成唯一数据库名
class SchemaEvolutionIT {

    @Container
    static MySqlContainer<?> mysql = new MySqlContainer<>(MySqlVersion.V8_0);

    @Test
    void testSchemaEvolution() {
        final String dbName = new UniqueDatabase(mysql, "schema_test").createNewDatabase();
        // 所有表都创建在 dbName 下，互不干扰
    }
}
```

### 3.4 禁用 Ryuk 清理器（CI 环境）

`pom.xml` surefire 配置中已设置：

```xml
<environmentVariables>
    <TESTCONTAINERS_RYUK_DISABLED>true</TESTCONTAINERS_RYUK_DISABLED>
</environmentVariables>
```

在 GitHub Actions CI runner 上，Ryuk 清理可能与容器网络冲突。生产环境（本地开发）无需设置。

---

## 四、端到端测试约定（FlinkSqlWDSTest）

### 4.1 assumeTrue 守卫机制

`FlinkSqlWDSTest` 需要两个预启动的 MySQL 实例。通过 `assumeTrue` 在 MySQL 不可用时跳过：

```java
@Test
void testFullCdcPipeline() {
    // 守卫条件：MySQL secrets 未配置则跳过
    final String sourceUrl = System.getProperty("mysql.it.source.url");
    final String sinkUrl = System.getProperty("mysql.it.sink.url");
    Assume.assumeTrue(
            sourceUrl != null && sinkUrl != null
                    && !sourceUrl.isEmpty()
                    && !sinkUrl.isEmpty());

    // given/when/then: full pipeline test
}
```

### 4.2 CI 中的 secrets 配置

在 GitHub 仓库 **Settings > Secrets** 中配置：

| Secret | 说明 |
|---|---|
| `MYSQL_IT_SOURCE_URL` | Source MySQL JDBC URL |
| `MYSQL_IT_SINK_URL` | Sink MySQL JDBC URL |
| `MYSQL_IT_SOURCE_USER` | Source 用户名 |
| `MYSQL_IT_SOURCE_PASSWORD` | Source 密码 |
| `MYSQL_IT_SINK_USER` | Sink 用户名 |
| `MYSQL_IT_SINK_PASSWORD` | Sink 密码 |

> **安全规则**：永远不要将真实密码写入代码或 `.env` 的提交版本。

---

## 五、测试数据管理

### 5.1 禁止硬编码生产数据

- 使用 JavaFaker 生成测试数据（姓名、UUID、邮箱等）
- 使用 Nacos/Properties 注入测试配置，不要硬编码

### 5.2 配置文件管理

测试资源文件放在 `src/test/resources/`：

```
src/test/resources/
├── test-config.properties     # 测试配置
└── log4j2-test.xml           # 测试专用日志配置
```

在测试代码中加载：

```java
@Test
void shouldLoadConfigFromResources() {
    final URL url = getClass().getClassLoader().getResource("test-config.properties");
    assertThat(url).isNotNull();
    final Properties props = new Properties();
    props.load(url.openStream());
    assertThat(props.getProperty("flink.checkpoint.interval")).isEqualTo("60000");
}
```

---

## 六、测试运行命令

### 6.1 本地开发

```bash
# 单元测试（排除集成测试）
./mvnw test -Dtest='!*IT,!*IntegrationTest,!*FlinkSqlWDSTest'

# 仅 flink-demo 单元测试
./mvnw -pl flink-demo test

# 运行特定测试类
./mvnw -pl cdc-mysql-sync test -Dtest=MysqlUtilTest

# 包含覆盖率报告
./mvnw verify -Djacoco.skip=false -pl cdc-mysql-sync
```

### 6.2 集成测试（需要 Docker）

```bash
# 运行所有集成测试（需要 Docker）
./mvnw test -pl cdc-mysql-sync -Dtest='*IT,*IntegrationTest'

# 运行端到端测试（需要 MySQL secrets）
./mvnw test -pl cdc-mysql-sync \
  -Dtest=FlinkSqlWDSTest \
  -DrunIntegrationTests=true \
  -Dmysql.it.source.url="jdbc:mysql://localhost:3306/source" \
  -Dmysql.it.sink.url="jdbc:mysql://localhost:3307/sink" \
  -Dmysql.it.source.user=root \
  -Dmysql.it.sink.user=root
```

### 6.3 CI 环境

```bash
# Job 1: lint + 单元测试（自动触发）
./mvnw -pl $MODULE -am test -Dtest='!*IT,!*IntegrationTest,!*FlinkSqlWDSTest'
./mvnw -pl $MODULE -am enforcer:enforce
./mvnw -pl $MODULE -am verify -Djacoco.skip=false -DskipTests

# Job 2: 集成测试（自动触发）
./mvnw -pl cdc-mysql-sync test -Dtest='*IT,!FlinkSqlWDSTest' -DrunIntegrationTests=true
```

---

## 七、测试覆盖目标

| 模块 | 当前覆盖率 | 阶段一目标（1个月） |
|---|---|---|
| `cdc-mysql-sync` | 10.1% | 25% |
| `flink-demo` | 0.0% | 20% |
| `flink-demo` | 100.0% | 保持 |

### 优先补充测试的类

1. `cdc-mysql-sync`: `BaseCode`、`ParameterUtil` — 工具类易测试
2. `cdc-mysql-sync`: `MysqlUtil` — 已有部分测试，补全边界条件
3. `cdc-mysql-sync`: `SchemaEvolver` — 核心业务逻辑
4. `flink-demo`: `ConfigUtil`、`RandomOptionGroup` — 无外部依赖

---

## 八、已知问题

### 8.1 flink-demo JaCoCo 覆盖率 0%

`flink-demo` 的 `IncrementMapFunctionTest` 运行成功，但主代码覆盖率显示 0%。可能原因：`provided` scope 依赖的 Flink 类在运行时未被正确 instrumented。**待调查**。

### 8.2 Flink CDC 事件难以构造

`DataChangeEvent`、`SchemaChangeEvent` 等 CDC 事件构造复杂，建议：
- 参考 `CdcEventDeserializerTest` 中的构造模式
- 在测试基础设施类 `MySqlSourceTestBase` 中提供辅助工厂方法
