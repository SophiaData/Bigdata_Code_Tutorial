# 编码规范

> 仓库已用 spotless + google-java-format 强制大部分格式。本文件补充**格式以外的**约定。

## 命名

| 类型 | 风格 | 例子 |
|---|---|---|
| 类 / 接口 | `PascalCase` | `FlinkSqlWDS`, `MysqlUtil` |
| 方法 / 变量 | `camelCase` | `createTable`, `sinkJdbcUrl` |
| 常量 | `UPPER_SNAKE_CASE` | `BATCH_SIZE`, `MYSQL_TIMESTAMP_DEFAULT` |
| 包名 | 全小写 | `io.sophiadata.flink.sync` |
| 测试方法 | `camelCase` + 描述性 | `createTable_emitsExpectedColumnsAndPrimaryKey` |

## 类 / 方法大小

- 类不超过 300 行（超出就拆）
- 方法不超过 50 行
- 嵌套类 ≤ 2 层

## 错误处理

- 不要吞异常：至少 `LOG.error(...)` 后再决定抛 / 吞
- `try-with-resources` 处理所有 `AutoCloseable`（Connection、Statement、InputStream）
- 自定义异常用包装：`throw new IllegalStateException("context", cause)`，不要直接抛 cause
- 在 catch 里**不要**空块 `{ }`，至少一条 log

## 日志

- 用 SLF4J：`LOG.info("Loaded {} keys from {}", n, path)`
- 占位符用 `{}`，不要用 `String.format`
- 不要拼字符串再传 `LOG.info(msg + "extra")`
- 异常用 `LOG.error("Failed: {}", sql, e)`，把异常当最后参数

## 并发

- 跨线程共享状态用 `ConcurrentHashMap` 或 `ConcurrentLinkedQueue`
- 不要在 lambda 里修改外部非 final 变量
- 线程池用 `Executors.newCachedThreadPool()` 时记得 `setDaemon(true)`
- 关闭线程池：`shutdown()` 后 `awaitTermination(timeout)`，不要 `shutdownNow()` 除非紧急

## SQL 生成

- 表名 / 列名一律反引号包裹 `` `name` ``
- 用户传入的标识符必须过 `isValidIdentifier()`（白名单正则 `[a-zA-Z0-9_]+`）
- 用 `PreparedStatement` 而不是字符串拼接，避免注入

## 配置 / ParameterTool

- 新增参数：在 `Constants` 加默认值 + 在 `ParameterUtil` 加 accessor
- 不要在业务代码里直接 `params.get("xxx")`，统一走 `ParameterUtil.xxx(params)`
- 配置前缀约定：`sinkXxx` 是 sink 端，`xxx` 是 source 端

## 测试

- 单元测试**只用 JUnit 5**（`org.junit.jupiter.api.Test`），不要再用 JUnit 4
- 测试方法命名：`<methodUnderTest>_<expectedBehavior>` 或 `<methodUnderTest>_<scenario>`
- 断言写**失败信息**：`assertTrue(sql.contains("BIGINT"), "missing BIGINT: " + sql)`
- 不要写 `Thread.sleep` 在单元测试里；集成测试可以但要短
- 集成测试用 `*IT` 后缀（surefire 默认会跑，需要 `-Dtest='!*IT'` 排除）

## 提交前自检

```bash
./mvnw spotless:apply
./mvnw -DskipTests=false -Dtest='!*IT,!*IntegrationTest,!*FlinkSqlWDSTest' test
```

## 不要做的事

- 不要写 `System.out.println` / `System.err.println`
- 不要 `import xxx.*;`
- 不要 catch 后只 `// ignore`
- 不要 hardcode URL / 账号 / 密码
- 不要 `git add .`（明确文件逐个加）
- 不要 commit `target/`、`*.class`、`.idea/`、`dependency-reduced-pom.xml`