# Flink Paimon Demo

Flink CDC → Apache Paimon 实时同步示例模块，支持 MySQL 和 MongoDB 数据源。

## 示例列表

| 示例 | 数据源 | 方式 | 入口类 |
|---|---|---|---|
| MySQL → Paimon | MySQL | SQL Catalog | `MySqlToPaimonPipeline` |
| MySQL → Paimon (SQL) | MySQL | SQL | `MySqlToPaimonSqlPipeline` |
| MongoDB → Paimon | MongoDB | DataStream | `MongoToPaimonPipeline` |
| MongoDB → Paimon (SQL) | MongoDB | SQL Catalog | `MongoToPaimonSqlPipeline` |

## 快速开始

### 环境要求

- JDK 11
- Maven 3.8+
- Docker（用于 Testcontainers 测试）

### 编译打包

```bash
# 切换 JDK 11
export JAVA_HOME=/path/to/jdk-11
export PATH="$JAVA_HOME/bin:$PATH"

# 编译
./mvnw clean package -DskipTests -pl cdc-paimon-sync -am
```

### 运行示例

#### MySQL → Paimon (DataStream)

```bash
flink run -c io.sophiadata.flink.paimon.MySqlToPaimonPipeline \
  target/cdc-paimon-sync-1.1.0.jar \
  --mysql.host localhost \
  --mysql.port 3306 \
  --mysql.database source_db \
  --mysql.username root \
  --mysql.password root \
  --paimon.path file:///tmp/paimon/catalog
```

#### MongoDB → Paimon (DataStream)

```bash
flink run -c io.sophiadata.flink.paimon.mongo.MongoToPaimonPipeline \
  target/cdc-paimon-sync-1.1.0.jar \
  --mongo.host localhost \
  --mongo.port 27017 \
  --mongo.database source_db \
  --mongo.username root \
  --mongo.password root \
  --paimon.path file:///tmp/paimon/catalog \
  --flatten.depth 0
```

**参数说明：**

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--mongo.host` | localhost | MongoDB 主机 |
| `--mongo.port` | 27017 | MongoDB 端口 |
| `--mongo.database` | (必填) | 源数据库名 |
| `--mongo.username` | root | 用户名 |
| `--mongo.password` | root | 密码 |
| `--mongo.collections` | (空=整库) | 指定集合，逗号分隔 |
| `--paimon.path` | file:///tmp/paimon/catalog | Paimon warehouse 路径 |
| `--flatten.depth` | 0 | 嵌套展平深度（0=JSON STRING） |

#### MongoDB → Paimon (SQL)

```bash
flink run -c io.sophiadata.flink.paimon.mongo.MongoToPaimonSqlPipeline \
  target/cdc-paimon-sync-1.1.0.jar \
  --mongo.host localhost \
  --mongo.port 27017 \
  --mongo.database source_db \
  --mongo.username root \
  --mongo.password root \
  --paimon.path file:///tmp/paimon/catalog \
  --mongo.collections "users,orders"
```

## MongoDB 类型映射

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
| Document | STRING (JSON) 或展平 |
| Null | NULL |

## 嵌套文档处理

通过 `--flatten.depth` 参数控制嵌套文档的处理方式：

- `0`（默认）：嵌套 Document 和 Array 存储为 JSON 字符串
- `1+`：按深度展平，支持 dot notation 列名（如 `address.city`）

## 测试

```bash
# 运行单元测试
./mvnw test -pl cdc-paimon-sync -Dtest='MongoTypeMapperTest,MongoDocumentFlattenerTest'

# 运行所有测试
./mvnw test -pl cdc-paimon-sync
```

## 依赖

- `flink-sql-connector-mongodb-cdc` — MongoDB CDC 连接器
- `mongodb-driver-sync` — MongoDB Java 驱动
- `paimon-flink-1.20` — Apache Paimon Flink 集成
- `paimon-flink-cdc` — Paimon CDC 支持
