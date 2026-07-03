# 项目结构

> 给 AI 用的"地图"。每改一处模块结构都要同步更新本文件。

## 顶层布局

```
Bigdata_Code_Tutorial/
├── pom.xml                          聚合 POM（Java 11，五个 module）
├── CLAUDE.md                        Claude 项目指令
├── README.md                        项目说明
├── LICENSE
├── .gitignore
├── docs/                            文档与 AI 上下文
│   ├── DEVELOPMENT.md               开发流程
│   └── ai-context/                  本目录
├── flink-demo/                      模块 1：DataStream/SQL/UDF 示例
├── sync_database_mysql/             模块 2：整库同步（核心）
├── flink-function/                  模块 3：可复用 Flink 函数
├── flink-paimon-demo/               模块 4：CDC → Paimon 数据湖同步
└── e2e-tests/                       模块 5：端到端集成测试
```

## flink-demo/

```
flink-demo/
├── pom.xml
└── src/
    ├── main/java/io/sophiadata/flink/
    │   ├── base/                    BaseCode、BaseSql 抽象
    │   ├── ddl/                     FlinkCDC DDL + Debezium 反序列化
    │   ├── glm/                     GLM API 测试（@Disabled）
    │   ├── source/                  MockSourceFunction、App* bean、配置
    │   │   └── utils/               ParamUtil、RandomNumString、ConfigUtil 等
    │   ├── sql/                     SQLTest 入口
    │   ├── streaming/               WordCount / Sideout / IncrementMapFunction
    │   └── udf/                     UDF 示例
    └── test/java/io/sophiadata/flink/
        ├── glm/                     GLMTest
        └── streaming/               JUnit 5 测试
```

## sync_database_mysql/

```
sync_database_mysql/
├── pom.xml
├── config.properties                本地配置模板（运行时覆盖）
└── src/
    ├── main/java/io/sophiadata/flink/sync/
    │   ├── FlinkSqlWDS.java         整库同步主入口
    │   ├── CdcEventDeserializer.java  Debezium SourceRecord → Flink CDC Event
    │   ├── CDBBatchSink.java        批量 JDBC Sink（upsert/delete）
    │   ├── SharedSchemaState.java   静态共享 schema（绕过 Flink 序列化）
    │   ├── base/BaseCode.java
    │   ├── schema/SchemaEvolver.java  处理 CDC schema 变更（CheckpointedFunction）
    │   └── util/
    │       ├── MysqlUtil.java       JDBC、建表 SQL 生成、类型映射
    │       ├── NacosUtil.java       Nacos / 本地配置合并
    │       ├── ParameterUtil.java   ParameterTool 字段读取
    │       └── PropertiesUtil.java  Properties 解析
    ├── main/resources/              log4j 配置、config.properties
    └── test/java/io/sophiadata/flink/
        ├── cdc/                     旧 CDC 集成测试（testcontainers）
        ├── sync/                    单元测试
        │   ├── CdcEventDeserializerTest.java  19 个测试
        │   ├── FlinkSqlWDSTest.java
        │   ├── schema/SchemaEvolverTest.java  19 个测试
        │   └── util/                MysqlUtilTest/NacosUtilTest/ParameterUtilTest/...
        └── utils/MySqlContainer.java  testcontainers 工具
```

## flink-function/

```
flink-function/
├── pom.xml                          独立打包（shade），Java 11
└── src/
    ├── main/java/com/zyzx/realtime/flink/function/
    │   └── SplitFunction.java       TableFunction 示例
    └── test/java/com/zyzx/realtime/flink/function/
        └── SplitFunctionTest.java   JUnit 5
```

## flink-paimon-demo/

```
flink-paimon-demo/
├── pom.xml                          shade jar，mainClass: MySqlToPaimonPipeline
├── docker-compose.yml               MySQL 8.4.0 + Flink 1.20
└── src/
    ├── main/java/io/sophiadata/flink/paimon/
    │   ├── mongo/                   MongoDB → Paimon（DataStream + SQL）
    │   └── mysql/                   MySQL → Paimon（DataStream + SQL）
    └── test/java/                   MongoTypeMapperTest、MongoDocumentFlattenerTest 等
```

## e2e-tests/

```
e2e-tests/
├── pom.xml                          仅测试，依赖前三个模块
└── src/test/java/
    ├── SchemaEvolutionIT.java       schema 变更端到端（Testcontainers MySQL）
    ├── CDBBatchSinkIT.java          批量写入测试（H2）
    ├── CreateMysqlLSinkTableIT.java sink 建表测试
    └── paimon/mongo/                MongoDB → Paimon E2E
```

## 关键依赖流向

```
sync_database_mysql
  ├─ flink-connector-mysql-cdc 3.6.0-1.20 (provided)
  ├─ flink-cdc-common 3.6.0-1.20
  ├─ flink-connector-jdbc 3.3.0-1.20
  ├─ mysql-connector-j 8.4.0
  ├─ nacos-client 2.1.2
  └─ flink-test-utils-junit (test)

flink-demo
  ├─ flink-streaming-java (provided)
  ├─ flink-table-api-*
  └─ flink-connector-kafka 3.3.0-1.20

flink-function
  ├─ flink-table-api-java-bridge
  └─ flink-table-planner

flink-paimon-demo
  ├─ flink-cdc-connect-mysql
  ├─ flink-paimon
  └─ flink-connector-mongodb (for mongo examples)
```

## 修改模块结构时的 checklist

1. 在根 `pom.xml` `<modules>` 段同步新增/删除
2. 如果新增 main 包，更新本文件
3. 如果改了核心依赖版本，更新 `docs/ai-context/project-structure.md` 和 `docs/DEVELOPMENT.md`
4. 提交前 `./mvnw spotless:apply` + `./mvnw test`
