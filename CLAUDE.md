# CLAUDE.md — Bigdata_Code_Tutorial

## 项目概览

Flink 1.20 大数据代码教程仓库，演示基于 flink-cdc 3.x 的 MySQL 整库实时同步。

- **GroupId**: `io.sophiadata`，版本 `1.0.0`
- **JDK**: 11（必须，class 文件版本 55.0）
- **构建**: Maven（仓库根 `./mvnw`）
- **持久上下文**: 详细模块说明见 [`docs/ai-context/`](docs/ai-context/)，开发流程见 [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md)

## 模块结构

| 模块 | 内容 |
|---|---|
| `flink-demo` | DataStream / SQL / CDC DDL / UDF / Mock 数据源 |
| `sync_database_mysql` | 整库同步：`flink-cdc 3.x` → `MySqlCatalog` → JDBC sink，含 `SchemaEvolver` |
| `flink-function` | 可复用 Flink `TableFunction` 示例 |

## 常用命令

```bash
# 切换 JDK 11（必须）
export JAVA_HOME=/Users/gaotingkai/Library/Java/JavaVirtualMachines/corretto-11.0.21/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

# 编译（跳过测试）
./mvnw -DskipTests package

# 跑单元测试（排除集成测试）
./mvnw test -Dtest='!*IT,!*IntegrationTest,!*FlinkSqlWDSTest'

# 仅跑某模块测试
./mvnw -pl sync_database_mysql -am test

# 自动修复 spotless / google-java-format
./mvnw spotless:apply
```

## 代码规范

- **格式**: google-java-format AOSP 风格（spotless 在 build 阶段强制）
- **导入顺序**: `org.apache.flink.*` → 空行 → `javax.*` → `java.*` → `scala.*` → 空行 → 其它
- **禁止通配符导入**: 不写 `import foo.*;`
- **License header**: 仓库根 `tool/license.header`，spotless 会强制
- **命名**: 类 `PascalCase`，方法/变量 `camelCase`，常量 `UPPER_SNAKE_CASE`
- **包名**: 主包 `io.sophiadata.flink.*`；`flink-function` 用 `com.zyzx.realtime.flink.function`
- **JUnit**: 新代码用 JUnit 5 Jupiter（`org.junit.jupiter.api.Test`）

## 测试约定

- 单元测试放 `src/test/java/`，包结构镜像 main
- 集成测试命名以 `IT` 结尾（`FlinkSqlWDSTest` 除外，是端到端）
- 集成测试默认被 surefire 跑（除非 `-Dtest='!*IT'` 排除）；需要 MySQL / Docker / 外部 URL

## Git 规范

- 提交信息格式：`<type>(<scope>): <description>`
  - 例：`feat(sync): add SchemaEvolver for CDC schema changes`
- **禁止**：`git push --force`、`git reset --hard`、`rm -rf`（全局 / ~ / 仓库根）
- 提交前必跑：`./mvnw spotless:apply`

## 安全规则

- **永远不要**把真实 API key / 数据库密码 / 私钥写入代码或 `.env` 的提交版本
- **永远不要**执行 `DROP DATABASE` / `DROP TABLE` / `TRUNCATE` 而不再次确认
- **永远不要**读 `~/.ssh/`、`~/.aws/`、`.env` 的密钥内容（即便只是为了展示）
- `config.properties` 模板可以提交，本地覆盖值不要提交

## 调试提示

- 测试报错 `class file version 55.0, only recognizes up to 52.0` → 当前是 JDK 8，切到 JDK 11
- spotless 失败 → `./mvnw spotless:apply`
- `FlinkSqlWDSTest` 报 `assumeTrue` 失败 → 那是集成测试，需要 `-DrunIntegrationTests=true` + MySQL URL
- 新增模块要在**根 `pom.xml`** 的 `<modules>` 段登记，否则 Maven 不会处理

## 风格偏好

- 回复使用中文（来自用户全局偏好）
- 技术名词、命令、路径、代码标识符保留英文不翻译
- 表格列名可中英混合