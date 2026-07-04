# Bigdata_Code_Tutorial

[Flink CDC](https://github.com/apache/flink-cdc) 整库实时同步示例仓库，基于 Flink 1.20 + flink-cdc 3.x，支持 MySQL → MySQL 和 MySQL → Paimon 两种同步模式。

[![CI](https://github.com/SophiaData/Bigdata_Code_Tutorial/actions/workflows/ci.yml/badge.svg)](https://github.com/SophiaData/Bigdata_Code_Tutorial/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/SophiaData/Bigdata_Code_Tutorial)](LICENSE)
[![Java](https://img.shields.io/badge/Java-11-blue?logo=openjdk)](https://adoptium.net/)
[![Flink](https://img.shields.io/badge/Flink-1.20.5-blue?logo=apache-flink)](https://flink.apache.org/)
[![Dependabot](https://img.shields.io/badge/Dependabot-enabled-brightgreen?logo=dependabot)](https://github.com/SophiaData/Bigdata_Code_Tutorial/network/dependencies)

## 模块

| 模块 | 说明 |
|---|---|
| [flink-demo](flink-demo/) | DataStream / SQL / CDC DDL / UDF / TableFunction 示例与 Mock 数据源 |
| [cdc-mysql-sync](cdc-mysql-sync/) | 整库同步核心：`flink-cdc 3.x` → JDBC sink，含 SchemaEvolver |
| [cdc-paimon-sync](cdc-paimon-sync/) | CDC → Apache Paimon 数据湖同步（MySQL + MongoDB） |
| [e2e-tests](e2e-tests/) | 端到端集成测试（Testcontainers） |

## 快速开始

### 环境要求

- Docker 和 Docker Compose（推荐）
- JDK 11+ + Maven 3.8+（本地开发）

### 🚀 一键 Docker 测试（推荐）

最简单的方式是使用 Docker Compose 一键启动所有服务：

```bash
# 1. 启动所有服务（MySQL + Flink）
chmod +x test-cdc.sh
./test-cdc.sh start

# 2. 访问 Web UI
# Flink UI: http://localhost:18081
# MySQL Source: localhost:33061
# MySQL Sink: localhost:33062

# 3. 运行测试
./test-ddl-changes.sh    # DDL 变更测试
./test-load.sh           # 性能测试

# 4. 停止服务
./test-cdc.sh stop
```

### 本地开发环境

#### 构建

```bash
# 切换到 JDK 11
export JAVA_HOME=/Users/gaotingkai/Library/Java/JavaVirtualMachines/corretto-11.0.21/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

# 编译（跳过测试）
./mvnw -DskipTests package

# 运行单元测试（排除集成测试）
./mvnw test -Dtest='!*IT,!*IntegrationTest,!*FlinkSqlWDSTest'

# 运行覆盖率报告
./mvnw verify -Djacoco.skip=false -pl cdc-mysql-sync
open cdc-mysql-sync/target/site/jacoco/index.html
```

#### 运行同步任务

修改 `config.properties` 中的 MySQL 连接信息后：

```bash
# 在 cdc-mysql-sync 目录下运行
java -cp target/cdc-mysql-sync-1.1.0-jar-with-dependencies.jar \
    io.sophiadata.flink.sync.FlinkSqlWDS \
    --config config.properties
```

详细说明见 [FlinkSqlWDS.java](cdc-mysql-sync/src/main/java/io/sophiadata/flink/sync/FlinkSqlWDS.java)。

## 测试和部署

### 🐳 Docker 一键测试

提供完整的 Docker 测试环境，包括：
- MySQL 源数据库和目标数据库
- Flink 集群
- 自动化测试脚本
- 性能测试工具

详细使用说明见 [DOCKER_TEST_GUIDE.md](DOCKER_TEST_GUIDE.md)。

### 🧪 测试类型

| 测试类型 | 脚本 | 说明 |
|---|---|---|
| 基础功能 | `./test-cdc.sh start` | 一键启动所有服务 |
| DDL 变更 | `./test-ddl-changes.sh` | 测试 Schema 变更同步 |
| 性能测试 | `./test-load.sh` | 大批量数据同步性能 |
| 集成测试 | `./mvnw test` | 单元测试和集成测试 |

### 🚀 CI/CD

| 工具 | 状态 | 说明 |
|---|---|---|
| CI/CD | ✅ GitHub Actions | lint + unit + IT + package + CodeQL |
| 依赖更新 | ✅ Dependabot | 每周自动创建依赖 PR |
| 代码格式化 | ✅ spotless | google-java-format AOSP 风格 |
| 代码规范 | ✅ checkstyle | 阿里巴巴 Java 规范 |
| 测试覆盖率 | ✅ JaCoCo | 覆盖率报告已配置 |
| 安全扫描 | ✅ CodeQL | Java 安全静态分析 |
| 依赖审计 | ✅ dependency-review | GitHub 原生依赖安全审计 |

详细工程化文档见 [docs/reliability/](docs/reliability/)。

## 核心流程

```
MySQL (Source) ──CDC──> Flink (cdc-mysql-sync) ──JDBC──> MySQL (Sink)
                           │
                           ├── SchemaEvolver: 自动处理 schema 变更
                           └── CDBBatchSink: 批量写入 + 按表分组
```

## 📖 文档

- [快速开始](DOCKER_TEST_GUIDE.md) - Docker 一键测试指南
- [开发指南](docs/DEVELOPMENT.md) - 本地开发流程
- [工程化文档](docs/reliability/) - CI/CD 和质量保证
- [Paimon 同步](cdc-paimon-sync/) - MySQL → Paimon 数据湖同步示例

## 🎯 核心特性

### ✅ 整库同步
- 自动发现源数据库所有表
- 目标表自动添加 `sink_` 前缀
- 支持数据库和表级别的过滤

### ✅ Schema 变更同步
- **无需重启任务**：ADD/MODIFY/DROP/RENAME COLUMN
- **无需重启任务**：CREATE/DROP TABLE
- **自动处理**：SchemaEvolver 自动同步变更

### ✅ 高性能
- **批量写入**：CDBBatchSink 按表批量处理
- **并行处理**：可配置并行度
- **检查点**：支持 Exactly-Once 语义

### ✅ 易测试
- **Docker 环境**：一键启动测试环境
- **自动化测试**：DDL 变更和性能测试
- **实时监控**：Flink Web UI 可视化监控

## 📚 大数据资料合集

扫码订阅 ima 知识库，获取大数据学习资料：

![大数据资料合集](docs/images/bigdata-qrcode.png)
