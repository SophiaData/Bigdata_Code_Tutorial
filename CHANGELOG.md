# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **JaCoCo 覆盖率追踪**：在根 `pom.xml` 中配置 `jacoco-maven-plugin 0.8.12`，绑定 `verify` 阶段，生成 `target/site/jacoco/` 报告
- **CI 强化**：`lint-and-unit` job 新增 Enforcer 依赖一致性检查和 JaCoCo 覆盖率收集
- **MySQL testcontainers 版本固定**：`MySqlVersion.V8_0` 固定为 `8.0.36`，避免 CI 环境中版本漂移
- **工程化文档**：`docs/reliability/` 目录新增可靠性分析报告、覆盖率报告、测试策略、发布流程四份文档
- **单元测试**：为 `flink-demo` 新增 4 个测试类（`ParamUtilTest`、`RandomOptionGroupTest`、`RandomNumTest`、`ConfigUtilTest`），覆盖 `source/utils` 工具类
- **checkstyle suppressions**：新增 `checkstyle/suppressions.xml`，对 CDC bean 类的 snake_case 字段豁免命名检查

### Changed
- **checkstyle failsOnError**：`pom.xml` 中从 `false` 改为 `true`，规范检查现在会阻断构建
- **spotless phase**：`checkstyle` 从 `validate` 移至 `verify` 阶段（与 spotless 同一 phase）
- **spotless ratchetFrom**：`pom.xml` 中移除 `ratchetFrom`，全量检查所有文件的格式规范
- **google-java-format**：1.7 → 1.15.0（兼容 JDK 11+，修复 JVM 11+ 兼容性警告）
- **jackson-annotations 版本**：`2.12.6` → `2.21`，解决与 `jackson-databind` 的版本冲突

### Fixed
- **FlinkSqlWDS 语法错误**（3 处缺括号）：`insertEvent()`、`deleteEvent()` 调用、`String.join()` 参数缺 `)`
- **WatermarkStrategy 导入路径**：`org.apache.flink.util.WatermarkStrategy` → `org.apache.flink.api.common.eventtime.WatermarkStrategy`（Flink 1.20 正确路径）
- **CDBBatchSink 泛型不匹配**：`sinkTo()` → `addSink()` + `RichSinkFunction<Event>` 泛型匹配（Flink 1.20 sink API）
- **SchemaEvolver checkstyle 违规**：修复 `mapToMysqlType()` 方法中 7 处 `if` 结构缺大括号
- **JsonStringDebeziumDeserializationSchema**：修复 `else if` 前 `}` 未在同一行的 RightCurly 违规
- **IncrementMapper 内部类**：提取为公开静态类，使单元测试可引用，提升覆盖率

### Dependencies
- 新增：`jacoco-maven-plugin 0.8.12`（test scope）

---

## [1.0.0] - 2023-09-17

> Initial release.

Initial release with:
- `cdc-mysql-sync` — Whole-database sync with flink-cdc 3.x, MySqlCatalog, SchemaEvolver, CDBBatchSink
- `flink-demo` — DataStream / SQL / CDC DDL / UDF examples with mock data sources
- `flink-demo` — Reusable Flink TableFunction samples
- Full CI pipeline with lint, unit tests, integration tests, shaded JAR packaging, and CodeQL security scanning
- Dependabot automated dependency updates
