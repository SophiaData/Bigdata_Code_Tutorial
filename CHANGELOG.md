# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

---

## [1.2.0] - 2026-07-07

### Added
- **Flink DataStream 高级示例**：新增 Kafka Source、JDBC Sink、Async I/O、Broadcast State、Interval Join、CoProcess、Timer、Dead Letter Queue、Exactly-Once 等高级用法
- **Flink SQL 高级示例**：新增 SQL CDC Source、CEP 复杂事件处理、Window 窗口、Lookup Join、去重等 SQL 示例
- **Kafka/ES Sink 示例**：新增 Kafka Sink 和 Elasticsearch Sink 教学示例
- **Pre-commit hooks**：新增 pre-commit 钩子，提交前自动执行 spotless 格式化检查
- **测试基础设施**：提取 `AbstractMysqlSyncIT` 基类，新增单元测试覆盖工具类
- **Flink SQL Window 示例**：新增 Flink SQL 窗口聚合示例

### Changed
- **Flink API 迁移**：将已废弃的 `setStateBackend()`/`setRestartStrategy()` 迁移为新 API（Flink 1.20 兼容）
- **代码质量修复**：跨多模块修复代码质量问题（#246）
- **依赖升级**：升级多个依赖版本，修复兼容性问题
- **文档修正**：修正文档中的错误配置和说明

### Fixed
- **Docker 测试环境 CDC 同步问题**：修复 Docker 环境下 CDC 同步测试失败的问题（#235）
- **P0 Bug 修复**：修复关键 Bug，升级依赖，修正文档（#236）

---

## [1.0.0] - 2023-09-17

> Initial release.

Initial release with:
- `cdc-mysql-sync` — Whole-database sync with flink-cdc 3.x, MySqlCatalog, SchemaEvolver, CDBBatchSink
- `flink-demo` — DataStream / SQL / CDC DDL / UDF examples with mock data sources
- `flink-demo` — Reusable Flink TableFunction samples
- Full CI pipeline with lint, unit tests, integration tests, shaded JAR packaging, and CodeQL security scanning
- Dependabot automated dependency updates
