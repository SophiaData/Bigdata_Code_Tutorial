# 测试覆盖率报告

> 生成时间：2026-06-29
> 工具：JaCoCo Maven Plugin 0.8.12
> 门槛：`minimum 0.00`（基线建立，尚未设置实际目标）

---

## 一、各模块覆盖率

| 模块 | LINE 覆盖 | BRANCH 覆盖 | 状态 |
|---|---|---|---|
| `cdc-mysql-sync` | **10.1%** (69/683) | 9.5% (26/273) | ⚠️ 基线已建立 |
| `flink-demo` | **100.0%** (6/6) | 100.0% | ✅ 充分覆盖 |
| `flink-demo` | **0.0%** (0/656) | — | 🔴 无覆盖 |

### 解读

- **cdc-mysql-sync**：主业务逻辑（`FlinkSqlWDS`、`SchemaEvolver`、`CdcEventDeserializer`）为 0%，被测试的仅是工具类（`MysqlUtil`、`NacosUtil` 等）
- **flink-demo**：虽然 `IncrementMapFunctionTest` 运行成功（1 passed），但主代码（`MockSourceFunction`、`AppConfig`、`RandomOptionGroup` 等）均未被测试覆盖
- **flink-demo**：小模块，SplitFunction 有测试，完全覆盖

---

## 二、cdc-mysql-sync 包级覆盖率

| 包 | 覆盖率 | 说明 |
|---|---|---|
| `io.sophiadata.flink.sync` | **0.0%** | FlinkSqlWDS 主类，无测试 |
| `io.sophiadata.flink.sync.schema` | **0.0%** | SchemaEvolver，无测试 |
| `io.sophiadata.flink.sync.util` | **45.3%** | MysqlUtil 等工具类部分覆盖 |
| `io.sophiadata.flink.sync.sink` | **低** | CreateMysqlLSinkTable，无测试 |

---

## 三、覆盖率门槛建议

当前 `jacoco-maven-plugin` 配置 `minimum 0.00`（零门槛，用于建立基线）。建议按阶段设定目标：

| 阶段 | 目标 LINE 覆盖 | 说明 |
|---|---|---|
| **当前基线** | 10% | 记录现状 |
| **阶段一（1个月）** | 25% | 补充 FlinkSqlWDS 工具方法测试 |
| **阶段二（2个月）** | 40% | 补充 SchemaEvolver、CDBBatchSink 测试 |
| **阶段三（3个月）** | 50% | 补充 flink-demo 核心 MockSourceFunction 测试 |

修改 `pom.xml` 中的 JaCoCo `minimum` 值即可收紧门槛：

```xml
<!-- 从 0.00 改为目标值，例如 0.25 表示 25% -->
<minimum>0.25</minimum>
```

---

## 四、JaCoCo Maven 命令参考

```bash
# 本地生成覆盖率报告（HTML）
./mvnw verify -Djacoco.skip=false -pl cdc-mysql-sync

# 查看报告（浏览器打开）
open cdc-mysql-sync/target/site/jacoco/index.html

# 仅检查覆盖率门槛（不生成报告）
./mvnw verify -Djacoco.skip=false

# CI 中覆盖率不通过则构建失败
# （需在 pom.xml 中设置 <minimum>0.25</minimum> 或更高）
```

---

## 五、JaCoCo 报告位置

```
cdc-mysql-sync/target/site/jacoco/index.html   ← 主覆盖率报告
flink-demo/target/site/jacoco/index.html        ← 已覆盖
flink-demo/target/site/jacoco/index.html            ← 零覆盖
```

---

## 六、JaCoCo 与 Maven 生命周期绑定说明

JaCoCo 插件绑定在根 `pom.xml` 的 `verify` 阶段：

| Execution | Phase | 说明 |
|---|---|---|
| `prepare-agent` | test（自动） | JVM agent 启动，数据写入 `jacoco.exec` |
| `report` | test | 生成 XML/HTML 报告 |
| `report-check` | verify | 校验覆盖率是否达到 `minimum` 门槛 |

`test` 阶段执行单元测试时自动触发覆盖率收集；`verify` 阶段做门槛校验。

---

## 七、已知限制

1. **flink-demo 0% 问题**：`flink-demo` 的 `IncrementMapFunctionTest` 运行了但主代码覆盖率仍为 0%，可能与 `provided` scope 依赖导致 instrumented class 路径问题有关，需进一步调查
2. **不区分 main/test**：JaCoCo 默认同时统计 main 和 test 代码，当前计数中包含了测试代码本身的行数
3. **CI 暂未上传覆盖率**：`.github/workflows/ci.yml` 已在 `lint-and-unit` job 中加入 `-Djacoco.skip=false`，但尚未配置覆盖率上传至 GitHub（如 Codecov/SonarQube）
