# 项目结构

> 给 AI 用的"地图"。每改一处模块结构都要同步更新本文件。

## 顶层布局

```
Bigdata_Code_Tutorial/
├── pom.xml                          聚合 POM（Java 11，三个 module）
├── CLAUDE.md                        Claude 项目指令
├── README.md                        项目说明
├── LICENSE
├── .gitignore
├── docs/                            文档与 AI 上下文
│   ├── DEVELOPMENT.md               开发流程
│   └── ai-context/                  本目录
├── flink-demo/                      模块 1：DataStream/SQL/UDF 示例
├── sync_database_mysql/             模块 2：整库同步（核心）
└── flink-function/                  模块 3：可复用 Flink 函数
```

## flink-demo/

```
flink-demo/
├── pom.xml
└── src/
    ├── main/java/io/sophiadata/flink/
    │   ├── base/                    BaseCode、BaseSql 抽象
    │   ├── ddl/                     FlinkCDC DDL + Debezium 反序列化
    │   ├── source/                  MockSourceFunction、App* bean、配置
    │   ├── sql/                     SQLTest 入口
    │   ├── streaming/               WordCount / Sideout / IncrementMapFunction
    │   └── udf/                     UDF 示例
    └── test/java/io/sophiadata/flink/
        └── streaming/               JUnit 5 测试
```

## sync_database_mysql/

```
sync_database_mysql/
├── pom.xml
├── config.properties                本地配置模板（track，运行时覆盖）
└── src/
    ├── main/java/io/sophiadata/flink/sync/
    │   ├── FlinkSqlWDS.java         整库同步主入口
    │   ├── base/BaseCode.java
    │   ├── common/Constants.java
    │   ├── schema/SchemaEvolver.java   处理 CDC schema 变更（AddColumn/DropColumn/AlterColumnType/RenameColumn/DropTable/Truncate）
    │   └── util/
    │       ├── MysqlUtil.java       JDBC、建表 SQL 生成
    │       ├── NacosUtil.java       Nacos / 本地配置合并
    │       ├── ParameterUtil.java   ParameterTool 字段读取
    │       └── PropertiesUtil.java  Properties 解析
    ├── main/resources/              log4j2 配置
    └── test/java/io/sophiadata/flink/
        ├── cdc/                     旧 CDC 集成测试（testcontainers）
        ├── sync/                    FlinkSqlWDSTest（新集成测试）
        │   └── util/                JUnit 5 单元测试（MysqlUtilTest/NacosUtilTest/...）
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

## 关键依赖流向

```
sync_database_mysql
  ├─ flink-connector-mysql-cdc 3.6.0-1.20 (provided)
  ├─ flink-cdc-common 3.6.0-1.20
  ├─ flink-sql-connector-mysql-cdc 3.6.0-1.20
  ├─ flink-connector-jdbc
  ├─ mysql-connector-java 8.0.30
  ├─ nacos-client 2.1.2
  └─ flink-test-utils-junit (test)

flink-demo
  ├─ flink-streaming-java (provided)
  ├─ flink-table-api-*
  └─ flink-connector-kafka

flink-function
  ├─ flink-table-api-java-bridge
  └─ flink-table-planner
```

## 修改模块结构时的 checklist

1. 在根 `pom.xml` `<modules>` 段同步新增/删除
2. 如果新增 main 包，更新本文件
3. 如果改了核心依赖版本，更新 `docs/ai-context/project-structure.md` 和 `docs/DEVELOPMENT.md`
4. 提交前 `./mvnw spotless:apply` + `./mvnw test`