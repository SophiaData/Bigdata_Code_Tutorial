# 发布流程

> 适用于 `Bigdata_Code_Tutorial` 所有模块

---

## 一、版本号规范

采用 **语义化版本（Semantic Versioning）**：

```
主版本.次版本.修订号   →   1.0.0
```

| 组成部分 | 变化条件 |
|---|---|
| **主版本（MAJOR）** | 不兼容的 API 变更 |
| **次版本（MINOR）** | 向后兼容的功能新增（如新增 Flink 连接器支持） |
| **修订号（PATCH）** | 向后兼容的问题修复（如 bug 修复、依赖升级） |

> 注意：对于教程项目，API 稳定性要求较低，但仍保持版本规范以便于用户依赖管理。

---

## 二、发布前 Checklist

每次 `git tag` 发布前，逐项检查以下条目：

### 2.1 代码质量

- [ ] `./mvnw spotless:apply` 通过，无格式违规
- [ ] `./mvnw test -Dtest='!*IT,!*IntegrationTest,!*FlinkSqlWDSTest'` 全部通过
- [ ] `./mvnw verify -Djacoco.skip=false` 单元测试通过 + 覆盖率报告生成
- [ ] `./mvnw enforcer:enforce` 无严重依赖冲突（允许 Flink 传递依赖的已知版本冲突）
- [ ] 所有 checkstyle 违规已修复（或确认可接受）

### 2.2 安全与依赖

- [ ] `dependabot.yml` 已收到本周依赖更新 PR，且已审查并合并
- [ ] 无高危 CVE 依赖（检查 GitHub Security > Dependabot alerts）
- [ ] `pom.xml` 中的版本号已更新（如需升级 Flink 版本）
- [ ] `pom.xml` 中的 Flink CDC 版本与 Flink 版本兼容（参考官方兼容性矩阵）

### 2.3 测试覆盖

- [ ] JaCoCo 覆盖率未显著下降（对比上一次发布的覆盖率基线）
- [ ] 新增代码有对应的单元测试

### 2.4 文档

- [ ] `CHANGELOG.md` 已更新本次发布内容（新增功能、修复、breaking changes）
- [ ] `README.md` 中的版本引用（如有）已更新
- [ ] `docs/DEVELOPMENT.md` 如有变更已同步更新

### 2.5 Git 状态

- [ ] 所有本地修改已提交（无 uncommitted changes）
- [ ] `git status` 显示干净的工作区
- [ ] 确认发布分支（通常为 `master`）

---

## 三、发布步骤

### 3.1 更新 CHANGELOG.md

在发布前手动维护 CHANGELOG.md（或使用 `conventional-changelog-maven-plugin` 自动生成）：

```markdown
## [1.1.0] - 2026-06-29

### Added
- `SchemaEvolver` 支持 CDC schema 变更自动同步
- 新增 MySQL 8.0.36 testcontainers 固定版本

### Fixed
- FlinkSqlWDS 编译错误（`WatermarkStrategy` 导入路径修复）

### Changed
- 升级 `google-java-format` 1.7 → 1.15.0
- 升级 `spotless-maven-plugin` 2.23.0 → 最新稳定版

### Dependencies
- Flink 1.20.0（不变）
- flink-cdc 3.6.0（不变）
```

### 3.2 创建 Git Tag

```bash
# 确认当前版本
VERSION=1.1.0

# 提交所有变更
git add -A
git commit -m "chore(release): prepare for $VERSION"

# 创建 tag
git tag -a v${VERSION} -m "Release $VERSION"
```

### 3.3 推送 Tag（触发 CI/CD）

```bash
# 仅推送 tag（CI 监听 tag 事件）
git push origin v${VERSION}

# 如果 CI 工作流需要也可以同时推送 commit
git push origin master
```

> ⚠️ **禁止使用 `git push --force`**

### 3.4 GitHub Release（手动创建）

1. 打开 GitHub 仓库页面
2. 点击 **Releases** > **Draft a new release**
3. 选择刚推送的 `v1.1.0` tag
4. 填写 Release Title（如 `v1.1.0 - SchemaEvolver 新增支持`）
5. 将 CHANGELOG.md 中本次发布的内容粘贴到描述区
6. 点击 **Publish release**

---

## 四、热修复流程（Hotfix）

适用于生产环境发现严重问题需要紧急修复的场景：

```
master ──●──●──●──●──●── ← hotfix/v1.0.1
              ↑
         从 v1.0.0 tag 切出
```

### 4.1 从上一个稳定 Tag 创建热修复分支

```bash
# 从上一个稳定 tag 切出 hotfix 分支
git checkout -b hotfix/v1.0.1 v1.0.0
```

### 4.2 修复并测试

```bash
# 在 hotfix 分支上修复问题
git add -A
git commit -m "fix!: resolve critical data loss bug in CDBBatchSink"

# 运行测试
./mvnw test -Dtest='!*IT'
```

### 4.3 打热修复 Tag

```bash
VERSION=1.0.1
git tag -a v${VERSION} -m "Hotfix $VERSION"
git push origin hotfix/v${VERSION}
git push origin v${VERSION}
```

### 4.4 合并回 master

```bash
git checkout master
git merge hotfix/v${VERSION} --no-ff
git push origin master
git branch -d hotfix/v${VERSION}
```

---

## 五、CI 发布检查（自动化）

以下检查已集成在 `.github/workflows/ci.yml` 中，每次 push 和 PR 自动运行：

| 检查项 | 状态 | 说明 |
|---|---|---|
| spotless:check | ✅ CI Job 1 | 格式检查 |
| Unit Tests | ✅ CI Job 1 | 排除 IT |
| Enforcer | ✅ CI Job 1 | 依赖一致性 |
| JaCoCo Coverage | ✅ CI Job 1 | 覆盖率收集 |
| Integration Tests | ✅ CI Job 2 | testcontainers MySQL |
| Package Shaded JAR | ✅ CI Job 3 | sync_database_mysql jar |
| CodeQL Security | ✅ CI Job 4 | 安全静态分析 |

> 发布前确保 CI 全部绿灯。

---

## 六、Dependabot 自动化依赖更新

项目已配置 Dependabot（`.github/dependabot.yml`），每周一 09:00（Asia/Shanghai）自动创建依赖更新 PR：

| 分组 | 更新策略 |
|---|---|
| `apache-flink` | 批量 minor/patch，同 Flink 兼容 |
| `flink-cdc` | 批量 minor/patch，与 Flink 同步 |
| `test` | 批量（junit、testcontainers、mockito） |
| `other-deps` | 批量其余依赖 |
| Flink major | **忽略**，需手动评估升级 |

合并 Dependabot PR 前：
- [ ] CI 全部通过
- [ ] 确认无 breaking change
- [ ] 更新 CHANGELOG.md（如有必要）
