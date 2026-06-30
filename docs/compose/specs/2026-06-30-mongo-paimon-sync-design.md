# MongoDB CDC → Paimon 同步设计

## [S1] 背景

在 flink-paimon-demo 模块中新增 MongoDB CDC → Apache Paimon 的实时同步示例。现有模块已有 MySQL → Paimon 的 SQL Catalog 方式示例，本次新增 DataStream + SQL 混合方式，支持 MongoDB 的灵活 schema 和嵌套文档。

## [S2] 目标

- 支持 MongoDB 整库同步和指定集合同步
- 支持嵌套文档展平（可配置深度）或 JSON STRING 存储
- 支持本地文件系统和 HDFS/S3 存储
- 自动处理 MongoDB schema 变更（新增字段）
- 提供 DataStream API 和 SQL 两种入口

## [S3] 架构

```
MongoDB (Change Streams)
    ↓
Flink CDC MongoDB Source (DataStream API)
    ↓
ProcessFunction: 类型转换 + 嵌套展平
    ↓
Paimon Sink (DataStream → Paimon)
```

## [S4] 参数设计

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--mongo.host` | localhost | MongoDB 主机 |
| `--mongo.port` | 27017 | MongoDB 端口 |
| `--mongo.database` | (必填) | 源数据库 |
| `--mongo.username` | root | 用户名 |
| `--mongo.password` | root | 密码 |
| `--mongo.collections` | (空=整库) | 指定集合，逗号分隔 |
| `--paimon.path` | file:///tmp/paimon/catalog | Paimon warehouse 路径 |
| `--flatten.depth` | 0 | 嵌套展平深度（0=JSON STRING） |

## [S5] 类型映射

| MongoDB 类型 | Paimon 类型 |
|---|---|
| ObjectId | STRING |
| String | STRING |
| Int32 | INT |
| Int64 | BIGINT |
| Double | DOUBLE |
| Decimal128 | DECIMAL(38,18) |
| Boolean | BOOLEAN |
| Date | TIMESTAMP(3) |
| Array | STRING (JSON) |
| Embedded Document | STRING (JSON) 或 ROW (可配置) |
| Null | NULL |

## [S6] 文件清单

| 文件 | 说明 |
|---|---|
| `MongoToPaimonPipeline.java` | DataStream 主入口，整库/指定集合同步 |
| `MongoToPaimonSqlPipeline.java` | SQL Catalog 方式示例 |
| `MongoTypeMapper.java` | MongoDB → Paimon 类型映射 |
| `MongoDocumentFlattener.java` | 嵌套文档展平逻辑 |
| `pom.xml` | 添加 MongoDB CDC 依赖 |
| `MongoToPaimonPipelineTest.java` | 单元测试 |

## [S7] 依赖

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-cdc-pipeline-connector-mongodb</artifactId>
    <version>${flink_cdc.version}</version>
</dependency>
```

## [S8] 测试策略

- 单元测试：MongoTypeMapper 类型映射验证、MongoDocumentFlattener 展平逻辑验证
- 集成测试：使用 Testcontainers 启动 MongoDB，验证端到端同步（可选）
