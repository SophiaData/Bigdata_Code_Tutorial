# 运行环境配置

本项目依赖 Flink CDC 进行 MySQL 整库同步，需要以下运行环境。

## 前置软件

| 软件 | 版本 | 必需 | 说明 |
|------|------|------|------|
| JDK | 11 | **是** | 项目使用 `<java.version>11</java.version>`，JDK 8 无法编译（class file version 55.0） |
| Maven | 3.8+ | **是** | 使用项目自带的 `./mvnw`，无需单独安装 |
| Docker | 20.10+ | 可选 | 仅运行集成测试（`*IT`、`FlinkSqlWDSTest`）时需要 |

### JDK 11 配置

```bash
# macOS 示例（使用 Corretto）
export JAVA_HOME=/Users/gaotingkai/Library/Java/JavaVirtualMachines/corretto-11.0.21/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

# 验证版本
java -version
# 期望输出：openjdk version "11.0.x"
```

> 如果使用 `direnv`，可以把上述 export 写入 `.envrc` 实现自动切换。

## MySQL 环境

项目需要 **两台 MySQL 实例**：

| 实例 | 端口 | 用途 | 示例数据库 |
|------|------|------|-----------|
| Source | 33061 | CDC 数据源 | `flink_source` |
| Sink | 33062 | 数据目标 | `flink_sink` |

### 快速启动（Docker Compose）

在项目根目录创建 `docker-compose.yml`：

```yaml
version: '3.8'
services:
  mysql-source:
    image: mysql:8.0
    container_name: mysql-source
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: flink_source
    ports:
      - "33061:3306"
    command:
      - --server-id=1
      - --binlog_format=ROW
      - --log_bin=mysql-bin
      - --binlog_row_image=FULL

  mysql-sink:
    image: mysql:8.0
    container_name: mysql-sink
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: flink_sink
    ports:
      - "33062:3306"
    command:
      - --server-id=2
```

启动：

```bash
docker-compose up -d

# 验证连接
mysql -h 127.0.0.1 -P 33061 -uroot -proot -e "SHOW DATABASES;"
mysql -h 127.0.0.1 -P 33062 -uroot -proot -e "SHOW DATABASES;"
```

### 源表建表语句

Source MySQL (33061) 中需要创建控制表 `table_process`：

```sql
CREATE DATABASE IF NOT EXISTS flink_source;
USE flink_source;

-- 控制表：定义表同步规则
CREATE TABLE IF NOT EXISTS table_process(
    source_table VARCHAR(200) NOT NULL,
    operate_type VARCHAR(200) NOT NULL,
    sink_type VARCHAR(200),
    sink_table VARCHAR(200),
    sink_columns VARCHAR(2000),
    sink_pk VARCHAR(200),
    sink_extend VARCHAR(200),
    slow_change VARCHAR(2000),
    PRIMARY KEY (source_table, operate_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 示例：同步 t_user 表的全量+增量数据
INSERT INTO table_process VALUES
('t_user', 'INSERT', 'mysql', 'jdbc_sink_t_user', '*', 'id', NULL, NULL),
('t_user', 'UPDATE', 'mysql', 'jdbc_sink_t_user', '*', 'id', NULL, NULL),
('t_user', 'DELETE', 'mysql', 'jdbc_sink_t_user', '*', 'id', NULL, NULL);

-- 示例源表
CREATE TABLE IF NOT EXISTS t_user(
    id BIGINT NOT NULL,
    name VARCHAR(255),
    age TINYINT,
    create_time TIMESTAMP(0) DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP(0) DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 目标表建表语句

Sink MySQL (33062) 中需要创建对应的目标表：

```sql
CREATE DATABASE IF NOT EXISTS flink_sink;
USE flink_sink;

CREATE TABLE IF NOT EXISTS jdbc_sink_t_user(
    id BIGINT NOT NULL,
    name VARCHAR(255),
    age TINYINT,
    create_time TIMESTAMP(0),
    update_time TIMESTAMP(0),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## 测试配置

### 环境变量（可选）

`cdc-mysql-sync` 模块支持从环境变量读取凭证：

```bash
export MYSQL_USERNAME=root
export MYSQL_PASSWORD=root
export MYSQL_SINK_USERNAME=root
export MYSQL_SINK_PASSWORD=root
```

### 运行测试

```bash
# 仅单元测试（不需要 MySQL）
./mvnw test -Dtest='!*IT,!*IntegrationTest,!*FlinkSqlWDSTest'

# 集成测试（需要 MySQL）
./mvnw test \
    -DrunIntegrationTests=true \
    -Dtest='FlinkSqlWDSTest' \
    -Dmysql.test.url='jdbc:mysql://localhost:33061/flink_source?useSSL=false&allowPublicKeyRetrieval=true' \
    -Dmysql.test.user=root \
    -Dmysql.test.password=root
```

> **testcontainers**：如果本地没有 MySQL，集成测试会自动使用 testcontainers 启动容器（速度较慢）。

## 依赖概览

| 模块 | 主要依赖 | 运行时需求 |
|------|---------|-----------|
| `flink-demo` | Flink CDC, Flink Streaming, Kafka Connector | Flink 集群 |
| `cdc-mysql-sync` | Flink CDC 3.x, MySQL Connector, JDBC | Flink 集群 + MySQL |
| `flink-demo` | TableFunction 示例 | 随其他模块使用 |

详细依赖关系见 `pom.xml` 中的 `dependencyManagement` 段。

## 常见问题

### 1. `class file version 55.0, only recognizes up to 52.0`

**原因**：使用了 JDK 8 编译项目。

**解决**：切换到 JDK 11。

### 2. 集成测试报 `assumeTrue` 失败

**原因**：缺少 `-DrunIntegrationTests=true` 参数。

**解决**：
```bash
./mvnw test -DrunIntegrationTests=true -Dmysql.test.url='jdbc:mysql://localhost:33061/...'
```

### 3. Docker 内存不足

**解决**：Docker Desktop → Settings → Resources → 分配至少 4GB 内存。

### 4. MySQL 连接被拒绝

**检查**：
```bash
# 检查容器状态
docker ps | grep mysql

# 检查端口监听
lsof -i :33061
lsof -i :33062
```

### 5. CDC 抓不到 binlog

**检查 MySQL 配置**：
```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';
```

期望：`log_bin=ON`，`binlog_format=ROW`，`binlog_row_image=FULL`。
