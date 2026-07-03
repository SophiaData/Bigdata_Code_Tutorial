# Development Guide

If something here disagrees with what `mvn` actually does, update the doc — the build is the source of truth.

## Prerequisites

| Tool   | Version | 何时需要 |
|--------|---------|------|
| JDK    | **11**  | 永远。父 POM `<java.version>11</java.version>`，JDK 8 跑不动（class 文件 55.0） |
| Maven  | 3.8+    | 永远。用仓库自带的 `./mvnw` |
| Docker | optional | 只跑 `*IT` / `FlinkSqlWDSTest` 时需要 |

### 切到 JDK 11

```bash
export JAVA_HOME=/Users/gaotingkai/Library/Java/JavaVirtualMachines/corretto-11.0.21/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
java -version   # 必须 11.x
```

> 上面这台机器上的 Azul 17 / Corretto 11 / Zulu 8 都装着。如果用 `direnv` 或 `.envrc` 想自动切，把这两行 export 放进去。

### IDEA 项目设置

`pom.xml` 设了 `<java.version>11</java.version>`，但 IDEA 自己存的 `.idea/misc.xml` 还有 `languageLevel` 和 `project-jdk-name`，**两边不一致就会出现 "源发行版 11 需要目标发行版 11" 警告**。

第一次打开项目时手动设一次：

1. **File → Project Structure → Project**
   - Project SDK: 选 `Corretto 11.0.21`（或任何 JDK 11）
   - Project language level: `11`
2. **File → Project Structure → Modules → 每个模块 → Sources tab**
   - Language level: `11`
3. **Build → Rebuild Project**（触发 IDEA 重读设置）

之后别再让 IDEA 自动跳回 1.8。`.idea/misc.xml` 在 `.gitignore` 里，改了不会影响其他人——每个开发者第一次 clone 后都要在 IDEA 里设一遍。

## 测试分层 —— 按改动范围选

不要每次都跑全部。集成测试单次 ~3-5 分钟，没必要时别付这个代价。

### 场景 1：日常改代码（默认就这个）

```bash
./mvnw -pl cdc-mysql-sync -am test
```

跑：
- `MysqlUtilTest` / `NacosUtilTest` / `ParameterUtilTest` / `PropertiesUtilTest` / `IncrementMapFunctionTest` / `SplitFunctionTest`
- 耗时 ~10 秒

跳过：
- `CreateMysqlLSinkTableIT`（surefire 默认不跑 `*IT`）
- `FlinkSqlWDSTest`（同上）

### 场景 1.5：本地跑某个 `flink-demo` 主类

**不要直接 IDEA Run**——`flink-*` 都是 `provided` scope，IDEA 默认不把它们加进运行时 classpath，结果启动时 `NoClassDefFoundError`、JVM exit 0、看不到任何输出。

用 `bin/run-demo.sh`：

```bash
bin/run-demo.sh io.sophiadata.flink.streaming.IncrementMapFunction
bin/run-demo.sh io.sophiadata.flink.streaming.Sideout
```

脚本会：
1. 强制切到 JDK 11（忽略外层 `JAVA_HOME`，因为经常是 zulu-8）
2. 通过 `dependency:build-classpath` 拿全 test scope 的 jar（含 provided）
3. 用绝对路径 java + 全 classpath 跑

> `handle()` 方法**不要**自己调 `env.execute()`，那由 `BaseCode.init` 负责。调两次会清空 transformations 列表，第二次抛 `IllegalStateException: No operators defined in streaming topology`。

### 场景 2：改了 CDC / SchemaEvolver / FlinkSqlWDS / createTable

```bash
./mvnw -pl cdc-mysql-sync -am test \
    -DrunIntegrationTests=true \
    -Dtest='MysqlUtilTest,NacosUtilTest,PropertiesUtilTest,ParameterUtilTest,FlinkSqlWDSTest,CreateMysqlLSinkTableIT' \
    -Dmysql.test.url='jdbc:mysql://localhost:33061/flink_source?useSSL=false&allowPublicKeyRetrieval=true' \
    -Dmysql.test.user=root \
    -Dmysql.test.password=root \
    -Dmysql.it.source.url='jdbc:mysql://localhost:33061/flink_source?useSSL=false&allowPublicKeyRetrieval=true' \
    -Dmysql.it.source.user=root \
    -Dmysql.it.source.password=root \
    -Dmysql.it.sink.url='jdbc:mysql://localhost:33062/flink_sink?useSSL=false&allowPublicKeyRetrieval=true' \
    -Dmysql.it.sink.user=root \
    -Dmysql.it.sink.password=root
```

跑：
- 14 个单元测试 +
- `CreateMysqlLSinkTableIT`（真 MySQL 跑 DDL，2 个 case）
- `FlinkSqlWDSTest`（MiniCluster + 真 MySQL，端到端）
- 合计 17 个 case，耗时 ~3-5 分钟

**前置**：本机有 MySQL source (33061) 和 sink (33062) 在跑（testcontainers 会自己起 MySQL 但慢；用已经跑着的 MySQL 更快）。没有的话 `CreateMysqlLSinkTableIT` 会自动 fallback 到 testcontainers。

> 注意：surefire 默认**不跑 `*IT.java`**（那是 failsafe 的活），所以必须显式 `-Dtest=...IT`。漏了的话会"通过"但其实 IT 没跑——看起来绿其实没验证。

### 场景 3：只跑某个测试

```bash
./mvnw -pl cdc-mysql-sync -am test -Dtest=MysqlUtilTest
./mvnw -pl cdc-mysql-sync -am test -Dtest='MysqlUtilTest,NacosUtilTest'
./mvnw -pl cdc-mysql-sync -am test -Dtest=FlinkSqlWDSTest -DrunIntegrationTests=true \
    -Dmysql.it.source.url='...' -Dmysql.it.sink.url='...'
```

### 场景 4：编译 / 打包

```bash
# 编译（跳过测试）
./mvnw -DskipTests package

# 仅打 cdc-mysql-sync 的可执行 jar
./mvnw -pl cdc-mysql-sync -am clean package -DskipTests
# 产物：cdc-mysql-sync/target/cdc-mysql-sync-1.1.0.jar
```

### 场景 5：CI（GitHub Actions / Jenkins）

`./mvnw verify -Pintegration` 之类 —— CI 上挂上场景 2 的命令，每次 push 跑一次完整集成。本仓库已通过 GitHub Actions 接入 CI（见 `.github/workflows/ci.yml`）。

## 代码风格

build 阶段 spotless 强制 google-java-format AOSP：

- 禁止 `import xxx.*;`
- 导入顺序：`org.apache.flink.*` → 空行 → `javax.*` → `java.*` → `scala.*` → 空行 → 其它
- License header 用 `tool/license.header`

```bash
# 本地自动修
./mvnw spotless:apply
```

## 配置优先级（`cdc-mysql-sync`）

```
CLI args  >  --config=file.properties  >  --nacos_server  >  Constants 默认值
```

CLI 始终胜出。`Constants` 是兜底，读取 `MYSQL_USERNAME` / `MYSQL_PASSWORD` / `MYSQL_SINK_USERNAME` / `MYSQL_SINK_PASSWORD` 环境变量。

## 排错

### `class file version 55.0, only recognizes up to 52.0`

JDK 8。切到 JDK 11。

### spotless 失败

```bash
./mvnw spotless:apply
```

### `FlinkSqlWDSTest` 报 `assumeTrue` 失败

缺 `-DrunIntegrationTests=true` + MySQL URL。要么按场景 2 跑，要么 `-Dtest='!*FlinkSqlWDSTest'` 跳过。

### `... is not serializable`（Flink job 提交失败）

详见 [`docs/ai-context/architecture.md`](ai-context/architecture.md) 的 "已知坑" 段。最常见的是 ProcessFunction / SinkFunction 里捕获的对象含 lambda ThreadFactory 或 ThreadPoolExecutor。

## 目录约定

```
src/main/java/...           生产代码
src/test/java/...           单元测试（JUnit 5 Jupiter）
src/test/java/.../*IT.java  集成测试（surefire 不跑，failsafe 才跑）
docs/                       文档与 AI 上下文
tool/license.header         Spotless license 模板
```