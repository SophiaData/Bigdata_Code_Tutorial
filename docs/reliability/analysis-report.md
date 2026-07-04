# 项目可靠性分析报告

> 生成时间：2026-06-29
> 覆盖范围：flink-demo、cdc-mysql-sync、cdc-paimon-sync 三个模块

---

## 一、现状总览

### 1.1 代码规模

| 模块 | 主代码 (.java) | 测试代码 (.java) | 测试/代码比 |
|---|---|---|---|
| flink-demo | 30 | 1 | 3.3% |
| cdc-mysql-sync | 8 | 10 | **125%** |
| cdc-paimon-sync | 1 | 1 | 100% |
| **合计** | **39** | **14** | **35.9%** |

> cdc-mysql-sync 测试文件多是因为包含 testcontainers 基础设施类（MySqlContainer、UniqueDatabase 等），实际业务测试覆盖率仍偏低。

### 1.2 质量基础设施

| 类别 | 状态 | 备注 |
|---|---|---|
| CI/CD（GitHub Actions） | ✅ 完善 | 4 个并行 job（lint+单元、集成、package、CodeQL） |
| 自动依赖更新（Dependabot） | ✅ 完善 | 分组策略 + Flink major 版本忽略规则 |
| 代码格式化（spotless） | ✅ 强制 | compile 阶段自动 check |
| 代码规范（Checkstyle） | ⚠️ 被动 | validate 阶段，failsOnError=false |
| 许可证头检查 | ✅ 强制 | spotless 集成 |
| 安全扫描（CodeQL） | ✅ 开启 | 独立 job |
| 依赖版本一致性（Enforcer） | ⚠️ 部分 | 仅 fail=false，CI 未执行 |
| 依赖安全审计（dependency-review） | ✅ 开启 | GitHub 原生 |
| 测试覆盖率追踪 | ❌ 缺失 | 无 jacoco 配置 |
| 发布流程文档 | ❌ 缺失 | 无正式流程 |

---

## 二、差距分析（Gap Analysis）

### 2.1 高优先级（影响可靠性）

#### G1 — 测试覆盖率不透明
- **现状**：项目没有配置 jacoco 或任何覆盖率工具，团队无法量化测试充分性
- **风险**：重构和依赖升级缺乏信心，改错不报错
- **建议**：在根 pom.xml 加入 jacoco-maven-plugin，定期生成 site 报告

#### G2 — Enforcer Plugin 未在 CI 中执行
- **现状**：Maven Enforcer Plugin 配置在 pom.xml，但 `fail=false`，且 validate 阶段在 checkstyle 之前，CI 没有显式运行
- **风险**：依赖版本冲突、CVE 版本漏检不会阻断构建
- **建议**：移到 verify 阶段，fail=true，CI 中显式运行

#### G3 — flink-demo 覆盖率极低
- **现状**：30 个主代码文件只有 1 个测试文件（IncrementMapFunctionTest）
- **风险**：MockSourceFunction、AppConfig、RandomOptionGroup 等核心工具类无测试
- **建议**：为 ConfigUtil、ParamUtil、RandomOptionGroup 等补充单元测试

### 2.2 中优先级（提升工程化）

#### G4 — Checkstyle failsOnError=false
- **现状**：规范检查不阻断构建，形同虚设
- **建议**：先修复现有 checkstyle 违规，再改为 failsOnError=true

#### G5 — testcontainers MySQL 版本不固定
- **现状**：cdc-mysql-sync pom.xml 中 testcontainers-mysql 依赖未指定具体版本
- **风险**：CI 环境和本地环境 MySQL 版本不一致，可能导致测试通过但生产失败
- **建议**：在根 pom.xml 的 dependencyManagement 中固定版本

#### G6 — 缺少变更日志规范
- **现状**：无 CHANGELOG.md，无 commit 规范强制（仅 CLAUDE.md 建议）
- **建议**：引入 conventional-changelog-maven-plugin 或维护 CHANGELOG.md

#### G7 — README 缺少 badge
- **现状**：README 只有 CI badge 和 License badge，缺少 coverage、Dependabot、Language 版本等
- **建议**：补充 Maven Central badge、Java 版本 badge、CI 分 job 状态

#### G8 — 缺少 monorepo 构建矩阵
- **现状**：CI 对 3 个模块分别运行，缺少一次性完整构建报告
- **建议**：增加汇总的 test/compile 结果汇总 job

### 2.3 低优先级（工程化完善）

#### G9 — 文档分散
- **现状**：CLAUDE.md、DEVELOPMENT.md、AI context 文档、docs/ 目录并存
- **建议**：合并到统一的 docs/ 目录，CLAUDE.md 仅保留快速参考

#### G10 — 缺少贡献指南
- **现状**：无 CONTRIBUTING.md
- **建议**：补充 fork/PR/review 流程

---

## 三、风险矩阵

| ID | 风险 | 可能性 | 影响 | 优先级 |
|---|---|---|---|---|
| G1 | 测试覆盖率不透明 | 高 | 高 | P0 |
| G2 | Enforcer 未强制 | 高 | 高 | P0 |
| G3 | flink-demo 覆盖率极低 | 高 | 高 | P0 |
| G4 | Checkstyle 不阻断 | 中 | 低 | P1 |
| G5 | testcontainers MySQL 版本漂移 | 中 | 高 | P1 |
| G6 | 无变更日志规范 | 低 | 中 | P2 |
| G7 | README badge 不全 | 低 | 低 | P2 |
| G8 | monorepo 构建矩阵缺失 | 低 | 中 | P2 |

---

## 四、改进路线图

```
阶段一（立即）：建立可观测性
├── T-1: 配置 jacoco-maven-plugin，建立覆盖率基线
├── T-2: 将 Enforcer 移到 verify 阶段，CI 显式执行
└── T-3: 固定 testcontainers MySQL 版本

阶段二（本周）：补测试
├── T-4: flink-demo 核心工具类单元测试（ConfigUtil、ParamUtil、RandomOptionGroup）
├── T-5: 修复 checkstyle 违规，改为 failsOnError=true
└── T-6: cdc-mysql-sync 缺失的工具类测试（BaseCode、SchemaEvolver）

阶段三（下周）：完善工程化
├── T-7: 补充 README badge
├── T-8: 编写 docs/reliability/testing-strategy.md
├── T-9: 编写 docs/reliability/release-process.md
└── T-10: 引入 CHANGELOG.md 规范

持续：Dependabot 依赖更新 + CI 监控
```

---

## 五、附录

### A. 相关文件路径

```
Bigdata_Code_Tutorial/
├── .github/
│   ├── workflows/
│   │   ├── ci.yml                  # 主 CI 工作流
│   │   ├── dependency-review.yml   # 依赖安全审计
│   │   ├── stale.yml               # 自动化关闭过期 issue
│   │   └── greetings.yml           # PR/issue 自动问候
│   └── dependabot.yml              # 自动化依赖更新策略
├── pom.xml                         # 根 pom，定义所有版本和插件
├── CLAUDE.md                       # 项目规范（开发者视角）
├── docs/
│   ├── ai-context/
│   │   ├── architecture.md
│   │   ├── coding-standards.md
│   │   └── project-structure.md
│   ├── DEVELOPMENT.md              # 开发流程
│   └── ENVIRONMENT.md              # 环境配置
├── checkstyle/
│   └── checkstyle.xml             # 阿里巴巴 Java 规范
└── tool/
    └── license.header             # spotless 许可证头
```

### B. CI 现状

CI 当前包含 4 个并行 job：
1. **lint-and-unit**（×3 模块）：spotless:check + 单元测试（排除 IT）
2. **integration-tests**：testcontainers MySQL + FlinkSqlWDSTest（可选）
3. **package-sync-database**：构建 shaded jar
4. **codeql**：Java 安全静态分析

**缺口**：无覆盖率收集、无 Enforcer 显式执行、无完整依赖树输出。

### C. 测试文件清单

| 文件路径 | 类型 | 备注 |
|---|---|---|
| cdc-mysql-sync/.../cdc/MySqlSourceExampleTest.java | 单元 | 示例测试 |
| cdc-mysql-sync/.../cdc/MySqlSourceTestBase.java | 测试基类 | testcontainers 管理 |
| cdc-mysql-sync/.../sync/SchemaEvolutionIT.java | 集成测试 | |
| cdc-mysql-sync/.../sync/FlinkSqlWDSTest.java | 端到端 | assumeTrue 守卫 |
| cdc-mysql-sync/.../sync/util/ParameterUtilTest.java | 单元 | |
| cdc-mysql-sync/.../sync/util/NacosUtilTest.java | 单元 | |
| cdc-mysql-sync/.../sync/util/MysqlUtilTest.java | 单元 | |
| cdc-mysql-sync/.../sync/util/PropertiesUtilTest.java | 单元 | |
| cdc-mysql-sync/.../sync/sink/CreateMysqlLSinkTableIT.java | 集成测试 | |
| flink-demo/.../streaming/IncrementMapFunctionTest.java | 单元 | |
| flink-demo/.../SplitFunctionTest.java | 单元 | |

> 注：git status 显示 `IncrementMapFunctionTest.java` 和 `SplitFunctionTest.java` 已从 worktree 中删除，但磁盘上仍存在（可能处于未跟踪状态）。
