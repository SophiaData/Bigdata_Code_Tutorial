# 编码规范

## 核心原则

1. **显式优于隐式** — 不使用 Lombok，手写 getter/setter/constructor
2. **类型安全** — 使用泛型，避免 raw type 警告
3. **资源管理** — AutoCloseable 对象必须用 try-with-resources
4. **异常处理** — 不吞异常，保留堆栈信息
5. **CI 必须通过** — 提交前本地验证编译和测试

## 禁止事项

| 禁止 | 原因 | 替代方案 |
|---|---|---|
| Lombok 注解 | IDEA 无法识别生成代码，导致误报 | 手写 getter/setter |
| `x.size() > 0` | 不直观 | `!x.isEmpty()` |
| `"utf-8"` 字符串 | 编码不一致 | `StandardCharsets.UTF_8` |
| 空 catch 块 | 吞异常，隐藏 bug | 至少打日志 |
| `Thread.sleep` 循环 | 忙等待，浪费 CPU | `CountDownLatch` / `Awaitility` |
| 未关闭的流/连接 | 资源泄漏 | try-with-resources |
| `switch` 无 `default` | 遗漏分支 | 添加 default 分支 |
| 通配符导入 `import foo.*` | 命名空间污染 | 显式导入 |

## 命名规范

```java
// 类名：PascalCase
public class UserAccount { }

// 方法名：camelCase
public void getUserById() { }

// 变量名：camelCase
String userName;

// 常量：UPPER_SNAKE_CASE
public static final int MAX_RETRY_COUNT = 3;

// 包名：全小写
package io.sophiadata.flink.sync;

// 枚举字段：camelCase（不用下划线）
public enum Status {
    ACTIVE, INACTIVE, PENDING
}
```

## 资源管理

```java
// ✅ 正确：try-with-resources
try (Connection conn = DriverManager.getConnection(url);
     Statement stmt = conn.createStatement();
     ResultSet rs = stmt.executeQuery(sql)) {
    // 处理结果
}

// ❌ 错误：手动关闭
Connection conn = null;
try {
    conn = DriverManager.getConnection(url);
    // ...
} finally {
    if (conn != null) conn.close(); // 可能抛异常
}
```

## 异常处理

```java
// ✅ 正确：保留堆栈信息
try {
    // 业务逻辑
} catch (SQLException e) {
    LOG.error("Database error: {}", e.getMessage(), e); // 传入异常对象
    throw new RuntimeException("Failed to query database", e);
}

// ❌ 错误：丢失堆栈
try {
    // 业务逻辑
} catch (SQLException e) {
    throw new RuntimeException(e.getMessage()); // 堆栈丢失
}

// ❌ 错误：空 catch
try {
    // 业务逻辑
} catch (Exception e) {
    // 什么都没有
}
```

## Flink 特定规范

```java
// ✅ StreamExecutionEnvironment 不需要手动关闭
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.execute("Job Name"); // Flink 运行时管理生命周期

// ✅ 使用参数化日志（SLF4J）
LOG.info("Processing record: {}", record.getId()); // 不需要 if(log.isInfoEnabled())

// ❌ 不需要守卫
if (LOG.isInfoEnabled()) {
    LOG.info("Processing record: {}", record.getId()); // 多余
}
```

## CI 检查清单

提交前必须通过：

```bash
# 1. 格式检查
./mvnw spotless:check

# 2. 单元测试
./mvnw test -Dtest='!*IT,!*IntegrationTest,!*FlinkSqlWDSTest'

# 3. 静态分析（可选但推荐）
./mvnw pmd:check checkstyle:check
```

## IDE 配置建议

### IntelliJ IDEA
- 安装 CheckStyle-IDEA 插件
- 配置 `checkstyle/checkstyle.xml` 作为项目规则
- 开启 `Analyze → Inspections` 中的关键检查

### 禁用 Lombok 插件
- `Settings → Plugins → Lombok → Uninstall`
- 或禁用 `Annotation Processors → Lombok`

## 提交规范

```
<type>(<scope>): <description>

type: feat | fix | refactor | test | docs | ci | chore
scope: sync | demo | function | paimon | ci | deps
description: 简短描述（中文或英文）
```

示例：
- `fix(sync): 修复 DELETE 事件处理逻辑`
- `feat(paimon): 添加 MySQL → Paimon 同步示例`
- `refactor(demo): 移除 Lombok 依赖，使用显式代码`
