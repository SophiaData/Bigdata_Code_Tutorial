# Docker 镜像构建和使用指南

## 📦 Docker 镜像概述

本项目提供完整的 Docker 环境配置，包括：
- MySQL 8.4.0 源数据库和目标数据库
- Flink 1.20.1 集群（JobManager + TaskManager）
- 自动化测试工具和数据加载器

## 🚀 快速启动

### 1. 准备工作

确保您的系统满足以下要求：
- Docker 20.10+
- Docker Compose 2.0+
- 至少 4GB 可用内存
- 端口 33061、33062、18081 可用

### 2. 启动环境

```bash
# 克隆项目
git clone <repository-url>
cd Bigdata_Code_Tutorial

# 启动所有服务
./test-cdc.sh start

# 等待服务启动（约 2-3 分钟）
# 看到 "🎉 Flink CDC 整库同步环境已启动成功！" 表示启动完成
```

### 3. 验证部署

```bash
# 检查服务状态
./test-cdc.sh status

# 查看 Flink Web UI
open http://localhost:18081

# 测试数据库连接
mysql -h localhost -P 33061 -u cdc_user -p cdc_password flink_source
mysql -h localhost -P 33062 -u root -p root flink_sink
```

## 📊 服务架构

```
┌─────────────────┐    ┌─────────────────┐
│   MySQL Source  │    │   MySQL Sink    │
│   (Port 33061)  │    │   (Port 33062)  │
│   - flink_source│    │   - flink_sink  │
│   - cdc_user    │    │   - root        │
└─────────────────┘    └─────────────────┘
          │                       │
          └───────────┬───────────┘
                      │
          ┌─────────────────┐
          │   Flink Cluster │
          │   (Port 18081)  │
          │   - JobManager  │
          │   - TaskManager │
          └─────────────────┘
```

## 🧪 测试脚本详解

### 1. 主控制脚本 (`test-cdc.sh`)

```bash
# 启动所有服务
./test-cdc.sh start

# 停止服务
./test-cdc.sh stop

# 重启服务
./test-cdc.sh restart

# 查看日志
./test-cdc.sh logs

# 查看状态
./test-cdc.sh status
```

### 2. DDL 变更测试 (`test-ddl-changes.sh`)

测试各种 Schema 变更操作：

```bash
./test-ddl-changes.sh
```

测试内容包括：
- ✅ ADD COLUMN：新增列
- ✅ MODIFY COLUMN：修改列类型
- ✅ DROP COLUMN：删除列
- ✅ RENAME COLUMN：重命名列
- ✅ CREATE TABLE：创建新表
- ✅ DROP TABLE：删除表

### 3. 性能测试 (`test-load.sh`)

```bash
# 基础性能测试
./test-load.sh basic [记录数] [持续时间]

# 批量性能测试
./test-load.sh batch

# 压力测试
./test-load.sh stress [记录数] [持续时间]
```

示例：
```bash
# 1万条记录，5分钟测试
./test-load.sh basic 10000 300

# 10万条记录，10分钟压力测试
./test-load.sh stress 100000 600
```

## 🔧 配置说明

### 1. Docker Compose 配置

#### MySQL 源数据库 (`mysql-source`)
```yaml
image: mysql:8.4.0
environment:
  MYSQL_ROOT_PASSWORD: root
  MYSQL_DATABASE: flink_source
  MYSQL_USER: cdc_user
  MYSQL_PASSWORD: cdc_password
ports:
  - "33061:3306"
command:
  - "--server-id=1"
  - "--log_bin=mysql-bin"
  - "--binlog_format=ROW"
  - "--gtid_mode=ON"
```

#### MySQL 目标数据库 (`mysql-sink`)
```yaml
image: mysql:8.4.0
environment:
  MYSQL_ROOT_PASSWORD: root
  MYSQL_DATABASE: flink_sink
ports:
  - "33062:3306"
```

#### Flink 集群
```yaml
# JobManager
image: flink:1.20.1-scala_2.12-java11
ports:
  - "18081:8081"  # Web UI
  - "6123:6123"   # RPC

# TaskManager
image: flink:1.20.1-scala_2.12-java11
scale: 1
```

### 2. Flink 作业配置

#### 配置文件 (`docker-config.properties`)
```properties
# MySQL Source
hostname=mysql-source
port=3306
username=cdc_user
password=cdc_password
databaseName=flink_source
tableList=.*
serverTimeZone=Asia/Shanghai

# MySQL Sink
sinkUrl=jdbc:mysql=mysql-sink:3306/flink_sink
sinkUsername=root
sinkPassword=root

# Job Settings
setParallelism=2
cdcSourceName=mysql-cdc-whole-database
```

#### 批量处理配置
```java
// 在 CDBBatchSink 中配置
private final int batchSize = 1000;          // 批量大小
private final long batchIntervalMs = 1000;    // 批量间隔（毫秒）
```

## 📈 监控和调优

### 1. Flink Web UI 监控

访问 http://localhost:18081 查看：

- **作业状态**：检查作业是否正常运行
- **检查点**：监控检查点完成情况
- **数据速率**：Source/Sink 的处理速率
- **背压**：检查是否存在背压问题
- **内存使用**：TaskManager 的内存使用情况

### 2. 数据库监控

```bash
# 查看MySQL进程
docker-compose logs mysql-source
docker-compose logs mysql-sink

# 查看Flink日志
docker-compose logs flink-jobmanager
docker-compose logs flink-taskmanager

# 实时监控
docker-compose logs -f [service_name]
```

### 3. 性能调优参数

#### 批量处理优化
```properties
# 增加批量大小提高吞吐量
batchSize=2000

# 调整批量间隔
batchIntervalMs=500
```

#### 并行度调整
```properties
# 根据CPU核心数调整
setParallelism=4
```

#### 检查点配置
```properties
# 在Flink配置中调整
execution.checkpointing.interval=1min
execution.checkpointing.mode=EXACTLY_ONCE
state.backend=filesystem
state.checkpoints.dir=file:///tmp/flink/checkpoints
```

## 🐛 常见问题解决

### 1. 端口冲突

```bash
# 检查端口占用
lsof -i :33061
lsof -i :33062
lsof -i :18081

# 修改端口映射
# 编辑 docker-compose.yml，修改 ports 配置
```

### 2. 内存不足

```bash
# 检查内存使用
docker stats

# 增加Docker内存分配
# Docker Desktop -> Preferences -> Resources -> Memory
```

### 3. MySQL 连接失败

```bash
# 检查MySQL服务状态
docker-compose ps

# 查看MySQL错误日志
docker-compose logs mysql-source

# 重启MySQL服务
docker-compose restart mysql-source
```

### 4. Flink 作业失败

```bash
# 查看Flink日志
docker-compose logs flink-jobmanager

# 检查作业状态
docker-compose exec flink-jobmanager flink list

# 重启作业
docker-compose exec flink-jobmanager flink stop <job-id>
docker-compose exec flink-jobmanager flink run -d [jar-path]
```

## 🔨 自定义配置

### 1. 修改数据库配置

```yaml
# docker-compose.yml
mysql-source:
  environment:
    MYSQL_ROOT_PASSWORD: your_password
    MYSQL_DATABASE: your_database
    MYSQL_USER: your_user
    MYSQL_PASSWORD: your_password
```

### 2. 修改Flink配置

```yaml
# flink-compose.yml
flink-jobmanager:
  environment:
    - FLINK_PROPERTIES=jobmanager.rpc.address: jobmanager
    - FLINK_PROPERTIES=taskmanager.numberOfTaskSlots: 8
```

### 3. 添加自定义测试数据

```bash
# 在 docker-init/ 目录添加SQL脚本
# 服务启动时会自动执行
```

## 📚 相关资源

- [Docker Compose 官方文档](https://docs.docker.com/compose/)
- [Flink 官方文档](https://nightlies.apache.org/flink/flink-docs-release-1.20/)
- [MySQL CDC 连接器](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/connectors/mysql-cdc/)
- [Flink 性能调优](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/performance_tuning/)