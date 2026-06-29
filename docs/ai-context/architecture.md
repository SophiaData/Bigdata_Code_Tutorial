# 架构说明

## 整库同步流水线（sync_database_mysql/FlinkSqlWDS）

```
┌──────────────────┐  binlog   ┌────────────────────┐
│ MySQL Source     │ ────────► │ DebeziumSource     │
│ (CDC)            │           │ Function<Event>    │
└──────────────────┘           └────────┬───────────┘
                                        │ CDC Event stream
                            ┌───────────┴────────────┐
                            │                        │
                  SchemaChangeEvent            DataChangeEvent
                            │                        │
                            ▼                        ▼
                  ┌────────────────────┐  ┌─────────────────────┐
                  │ ProcessFunction    │  │ CDBBatchSink        │
                  │ → SchemaEvolver    │  │ (RichSinkFunction)  │
                  │   + 在线建/改表     │  │ → MySQL JDBC sink   │
                  └────────────────────┘  └─────────────────────┘
```

### 数据流细节

1. **Source**: `MySqlSource.builder()` 创建 Debezium source，监听整个 database（`db.*`）
2. **Deserializer**: `CDCEventDeserializer` 把 Debezium 的 `SourceRecord` 转成 `Event`
   - `op ∈ {c, r, u, d}` → `DataChangeEvent.insertEvent / updateEvent / deleteEvent`
   - `op == "t"` → `TruncateTableEvent`
   - `op == "d"` → `DropTableEvent`（schema change）
3. **ProcessFunction 分支**:
   - `SchemaChangeEvent` → `SchemaEvolver.processEvent` → 异步 ALTER TABLE
   - 维护 `schemas: Map<TableName, Map<ColName, Type>>` 和 `pks: Map<TableName, PkCol>`
   - 首次见到 `CreateTableEvent` 时自动 `CREATE TABLE IF NOT EXISTS` 到 sink
4. **Sink**: `CDBBatchSink`（`RichSinkFunction<DataChangeEvent>`）
   - 按表分批 → `INSERT ... ON DUPLICATE KEY UPDATE`
   - batch size 200 / flush interval 1000ms

### SchemaEvolver 处理的 schema 变更

| 事件 | 反应 |
|---|---|
| `CreateTableEvent` | `LOG.info`（表由 ProcessFunction 创建） |
| `AddColumnEvent` | 异步 `ALTER TABLE ... ADD COLUMN` |
| `DropColumnEvent` | 异步 `ALTER TABLE ... DROP COLUMN` |
| `AlterColumnTypeEvent` | 异步 `ALTER TABLE ... MODIFY COLUMN` |
| `RenameColumnEvent` | 异步 `ALTER TABLE ... CHANGE COLUMN` |
| `DropTableEvent` | `LOG.info`（无操作） |
| `TruncateTableEvent` | `LOG.info`（无操作） |
| `AlterTableCommentEvent` | `LOG.info`（无操作） |

### 配置优先级

```
CLI args  >  --config=file.properties  >  --nacos_server  >  Constants 默认值
```

CLI 始终胜出，文件/Nacos 仅填补缺失键。详细见 `NacosUtil.mergeInto`。

### 关键设计取舍

- **同步自动建表**: 不依赖 `table_process` 配置表，启动时直接从 CDC 事件学 schema
- **批写 sink**: 避免单条 INSERT 性能差；批量 + 间隔双触发
- **schema 演进异步**: ALTER TABLE 不阻塞主数据流
- **类型映射**: `CDC 类型 → MySQL 类型` 在 `mapToMysqlType()` 集中

## flink-demo 流水线

主要是教学示例，不构成生产流水线：
- `MockSourceFunction` 生成 APP 行为日志（启动/页面/曝光/动作）
- `SQLTest` / `FlinkCDCDDL` 演示 Flink SQL + CDC DDL 写法
- `streaming/WordCount` / `Sideout` / `IncrementMapFunction` 演示 DataStream API

## flink-function

独立的 UDF 库，与项目其他模块无依赖耦合。

## 已知坑

### SchemaEvolver 必须 Serializable

`SchemaEvolver` 在 `FlinkSqlWDS.handle()` 里被 `ProcessFunction` 的匿名内部类捕获（`finalEvolver`）。Flink 在 job submit 时把整个闭包序列化下发到 TaskManager —— 所以 `SchemaEvolver` 必须 `Serializable`。

**两个隐藏坑**：

1. **Lambda ThreadFactory**：`Executors.newCachedThreadPool(r -> { ... })` 的 `r -> {...}` 是 lambda，捕获 enclosing context，**不是 Serializable**。
   - 修法：抽成 `static final class ... implements ThreadFactory, Serializable`

2. **`ThreadPoolExecutor` 默认 `AbortPolicy`**：`ExecutorService` 接口没声明 `Serializable`，`ThreadPoolExecutor` 是 `Serializable` 但内含 `handler: RejectedExecutionHandler`，默认是 `AbortPolicy`（非 Serializable 内部类）。
   - 修法：把 `alterExecutor` 标 `transient`，构造器里初始化，再加 `readObject()` 反序列化时重建：

   ```java
   private transient ExecutorService alterExecutor;

   public SchemaEvolver(...) {
       ...
       this.alterExecutor = Executors.newCachedThreadPool(new SchemaAlterThreadFactory());
   }

   private void readObject(ObjectInputStream in) throws ... {
       in.defaultReadObject();
       this.alterExecutor = Executors.newCachedThreadPool(new SchemaAlterThreadFactory());
   }
   ```

### 同样的规则适用于 CDBBatchSink

`CDBBatchSink` 也是 `RichSinkFunction`，要 Serializable。里面的 lambda（`Collections.nCopies(...)`、`Collectors.joining(...)` 等）只在 sink 内部短暂使用，不被外层闭包捕获，所以默认 OK。但 `private static class Record` 也跟着 sink 序列化，里面的 `final DataChangeEvent event` —— `DataChangeEvent` 在 flink-cdc 里 Serializable，但下游改 schema 时如果给 `Record` 加 lambda 字段，就会再次踩同样的坑。

## 修改架构时的 checklist

1. 同步更新本文档的 Mermaid/ASCII 图
2. 同步更新 `FlinkSqlWDS.handle()` 的行为描述
3. 如果改了 schema 演进行为，更新 `SchemaEvolver` 表格
4. 如果改了配置优先级，更新 `NacosUtil` Javadoc + 这份文档