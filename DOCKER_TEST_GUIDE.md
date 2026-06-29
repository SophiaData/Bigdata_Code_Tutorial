# Flink CDC 整库同步测试指南

## 🚀 一键启动

### 前置要求
- Docker 和 Docker Compose 已安装
- 至少 4GB 内存可用
- 端口 33061、33062、18081 可用

### 快速开始

```bash
# 1. 克隆项目
git clone <repository-url>
cd Bigdata_Code_Tutorial

# 2. 启动所有服务
chmod +x test-cdc.sh
./test-cdc.sh start

# 3. 等待服务启动（约 2-3 分钟）
# 4. 开始测试！
```

## 📊 服务访问

| 服务 | 地址 | 用途 |
|------|------|------|
| Flink Web UI | http://localhost:18081 | 监控作业状态和指标 |
| MySQL Source | localhost:33061 | 源数据库 |
| MySQL Sink | localhost:33062 | 目标数据库 |

## 🧪 测试脚本

### 1. 基础功能测试
```bash
# 启动环境
./test-cdc.sh start

# 查看 Flink 作业状态
./test-cdc.sh status

# 查看日志
./test-cdc.sh logs
```

### 2. DDL 变更测试
```bash
# 测试各种 DDL 操作
./test-ddl-changes.sh
```

### 3. 性能测试
```bash
# 基础性能测试（1万条记录）
./test-load.sh basic 10000 300

# 批量性能测试（测试不同批量大小）
./test-load.sh batch

# 压力测试（10万条记录）
./test-load.sh stress 100000 600
```

### 4. 环境管理
```bash
# 停止所有服务
./test-cdc.sh stop

# 重启环境
./test-cdc.sh restart

# 查看容器状态
./test-cdc.sh status
```

## 📋 测试步骤

### 步骤 1：验证环境启动
1. 访问 http://localhost:18081 查看 Flink Web UI
2. 确认看到 "CDC-Whole-Database-Sync" 作业运行中
3. 检查 MySQL 连接：
   ```bash
   # 源数据库
   mysql -h localhost -P 33061 -u cdc_user -p cdc_password flink_source
   
   # 目标数据库
   mysql -h localhost -P 33062 -u root -p root flink_sink
   ```

### 步骤 2：数据同步验证
1. 在源数据库插入测试数据：
   ```sql
   INSERT INTO users (name, email, age) VALUES ('Test User', 'test@example.com', 25);
   INSERT INTO orders (user_id, product_name, amount) VALUES (1, 'Laptop', 999.99);
   ```
2. 等待 1-2 秒
3. 在目标数据库验证数据：
   ```sql
   SELECT * FROM sink_users WHERE name = 'Test User';
   SELECT * FROM sink_orders WHERE product_name = 'Laptop';
   ```

### 步骤 3：DDL 变更测试
运行 DDL 测试脚本：
```bash
./test-ddl-changes.sh
```

### 步骤 4：性能测试
```bash
# 运行性能测试
./test-load.sh basic 10000 300
```

## 🔍 预期结果

### 1. 整库同步
- ✅ 源数据库所有表自动同步到目标数据库
- ✅ 表名自动添加 `sink_` 前缀
- ✅ 数据实时同步，延迟 < 1 秒

### 2. DDL 变更同步
- ✅ ADD COLUMN：新增列同步
- ✅ MODIFY COLUMN：列类型修改同步
- ✅ DROP COLUMN：列删除同步
- ✅ RENAME COLUMN：列重命名同步
- ✅ CREATE/DROP TABLE：表结构同步
- ✅ 任务无需重启

### 3. 性能指标
- ✅ 1万条数据 < 2 分钟完成同步
- ✅ 10万条数据 < 10 分钟完成同步
- ✅ 批量写入优化，减少数据库压力

## 🐛 常见问题

### 1. 服务启动失败
```bash
# 检查端口占用
lsof -i :33061
lsof -i :33062
lsof -i :18081

# 清理容器
docker-compose down -v
./test-cdc.sh start
```

### 2. MySQL 连接失败
```bash
# 检查 MySQL 服务状态
docker-compose ps

# 查看 MySQL 日志
docker-compose logs mysql-source
docker-compose logs mysql-sink
```

### 3. Flink 作业失败
```bash
# 查看 Flink 日志
docker-compose logs flink-jobmanager
docker-compose logs flink-taskmanager

# 检查作业状态
./test-cdc.sh status
```

### 4. 数据同步延迟
```bash
# 检查 Flink 检查点状态
curl http://localhost:18081/jobs

# 调整批量配置
# 修改 docker-config.properties 中的批量参数
```

## 📈 监控和调优

### 1. Flink Web UI 监控
- 作业状态和并行度
- 检查点完成情况
- 数据处理速率
- 背压情况

### 2. 数据库监控
```sql
-- 源数据库监控
SHOW PROCESSLIST;
SHOW STATUS LIKE 'Threads%';

-- 目标数据库监控
SHOW PROCESSLIST;
SHOW STATUS LIKE 'Com_insert%';
```

### 3. 性能调优建议
- 调整批量大小：根据数据量调整 `batchSize`
- 调整并行度：根据 CPU 核心数调整 `setParallelism`
- 启用检查点：提高数据可靠性
- 监控内存使用：避免 OOM

## 🎯 测试场景

### 场景 1：正常业务数据同步
- 模拟日常业务数据插入、更新、删除
- 验证数据一致性和实时性

### 场景 2：Schema 变更
- 模拟业务发展中的表结构变更
- 验证变更同步的准确性和及时性

### 场景 3：高并发写入
- 模拟高峰期的大量数据写入
- 验证系统的稳定性和性能

### 场景 4：故障恢复
- 模拟服务中断和恢复
- 验证数据不丢失和恢复时间

## 📚 相关文档

- [Flink CDC 官方文档](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/)
- [MySQL CDC 配置说明](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/connectors/mysql-cdc/)
- [Flink 性能调优](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/performance_tuning/)