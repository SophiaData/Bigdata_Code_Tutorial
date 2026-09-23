# Bigdata_Code_Tutorial

Flink / Flink CDC 示例工程。用一个源码树同时支持 **Flink 1.20** 与 **Flink 2.x** 两条版本线，
两条线都使用 **Flink CDC 3.x**。

---

## 一、版本矩阵

| 版本线 | Flink | Flink CDC | JDBC 连接器 | 构建命令 |
|---|---|---|---|---|
| **1.20**（默认） | 1.20.0 | 3.6.0-1.20 | `flink-connector-jdbc:3.4.0-1.20` | `./build.sh 1.20` |
| **2.2** | 2.2.0 | 3.6.0-2.2 | `flink-connector-jdbc-mysql:4.1.0-2.2` | `./build.sh 2.2` |

```bash
./build.sh 1.20     # Flink 1.20 + CDC 3.x，跑 verify 与打包
./build.sh 2.2      # Flink 2.2 + CDC 3.x
./build.sh all      # 两条线都构建一遍，验证源码与版本无关
```

Windows 用 `build.cmd 1.20` / `build.cmd 2.2` / `build.cmd all`。

> **两条线都使用 CDC 3.x（`org.apache.flink.cdc.*`），所以不需要任何 CDC 兼容层。**

> **源码集需要单独的 `-P`**：版本 profile 只切换依赖坐标，源码集 profile 由
> `-Pflink20` / `-Pflink2` 选择。Maven 的 profile 激活不会向下传递到子模块，所以两者缺一不可。
> `build.sh` / `build.cmd` 已经帮你把该带的都带上了。
>
> 手动执行等价于：
> ```bash
> ./mvnw -Pflink-1.20 verify
> ./mvnw -Pflink-2.2 -Pflink2 -Pflink2-test-guava verify
> ```

### 两条线的差异

除了 Flink 版本，还有两处上游差异需要吸收：

| 差异点 | Flink 1.20 | Flink 2.x |
|---|---|---|
| JDBC 连接器坐标 | `flink-connector-jdbc` | `flink-connector-jdbc-mysql`（FLIP-449 拆分，聚合构件停在 3.4.0-1.20） |
| `flink-java` | 存在 | **已移除**（DataSet API 在 2.0 删除） |
| `ParameterTool` 包名 | `org.apache.flink.api.java.utils` | `org.apache.flink.util`（移入 flink-core） |
| 重启策略配置 | `env.setRestartStrategy(...)` | 已移除，改用 `Configuration` 声明式配置 |
| 状态后端配置 | `env.setStateBackend(...)` | 已移除，改用 `StateBackendOptions` |
| Checkpoint 存储 / 外部化 | `CheckpointConfig` 方法 | 已移除，改用 `CheckpointingOptions` |
| `ParallelSourceFunction` | 存在 | 已移除（`SourceFunction` 移到 `.legacy` 包） |

这些差异全部集中在 `flink-compat` 模块，业务代码不受影响。

---

## 二、JDK 要求

工程产出 **Java 11 字节码**，构建与测试使用 **JDK 11 或 17**。

> ⚠️ **不要用 JDK 18+ 运行测试。** Flink 1.20 的 `MiniCluster` 会调用 JAAS 的
> `Subject.getSubject()`，该 API 在 JDK 18 起被默认禁用，测试会抛
> `UnsupportedOperationException: getSubject is not supported`。
> 编译可以在更高 JDK 上进行，但 `mvn test` 请用 11/17。

---

## 三、模块结构

```
bigdata_code_tutorial
├── flink-compat            Flink 版本兼容层（唯一接触版本差异的地方）
│   └── src/main/
│       ├── java/           ParameterTool 门面 + FlinkCompat 门面
│       ├── flink20/java/   Flink 1.20 实现（api.java.utils / setRestartStrategy / ...）
│       └── flink2/java/    Flink 2.x 实现（util / Configuration 声明式配置 / ...）
├── flink-demo              Flink 算子 / SQL / UDF / 埋点日志 Mock 数据源
└── sync_database_mysql     MySQL 整库同步到 MySQL
```

### 多版本是怎么做到的

两条线都是 CDC 3.x，包名相同，因此**唯一需要兼容层的是 Flink 自身的 API 变更**：

1. **依赖坐标**由父 POM 的 profile 切换（`${flink.version}`、`${flink.cdc.version}`、
   `${flink.jdbc.artifact}` 等）。
2. **`ParameterTool`**：Flink 2.0 把 `flink-java` 整个移除，并把 `ParameterTool` 移到
   `flink-core`。`flink-compat` 提供同名门面类，两套 `FactoryImpl` 由源码集选择。
3. **其它被移除的 API**（重启策略 / 状态后端 / checkpoint 存储 / `ParallelSourceFunction`）：
   `flink-compat` 的 `FlinkCompat` 按线给出等价实现。
4. 业务代码只 import `io.sophiadata.flink.compat.*`，不直接依赖任何版本特定的 Flink API。

---

## 四、运行

### 1. 整库同步 `FlinkSqlWDS`

```bash
./mvnw -Pflink-1.20 exec:java \
  -Dexec.mainClass=io.sophiadata.flink.sync.FlinkSqlWDS \
  -Dexec.args="--hostname localhost --port 3306 --databaseName test \
               --tableList .* --username root --password 123456 \
               --sinkUrl 'jdbc:mysql://localhost:3306/test2?serverTimezone=Asia/Shanghai' \
               --sinkUsername root --sinkPassword 123456"
```

参数说明：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `hostname` / `port` | `localhost` / `3306` | 源库地址 |
| `databaseName` | `test` | 源库名 |
| `tableList` | `.*` | `.*` 表示整库；也可写 `a,b` 或 `db.a,db.b` |
| `username` / `password` | `root` / `123456` | 源库账号 |
| `sinkUrl` / `sinkUsername` / `sinkPassword` | 见 `Constants` | 目标库 |
| `sinkPrefix` | `sink_%s` | 目标表名前缀，**必须含 `%s`** |
| `setParallelism` | `2` | 并行度 |

> `sinkPrefix` 会同时用于建表和 INSERT。早期版本两处不一致，传入自定义前缀会导致作业找不到表，
> 现已修正并加了参数校验。

**前置条件**：源表必须有主键（MySQL CDC 不支持无主键表）；目标库需可写。

### 2. CDC 变更日志

```bash
./mvnw -Pflink-1.20 exec:java \
  -Dexec.mainClass=io.sophiadata.flink.ddl.FlinkCDCDDL \
  -Dexec.args="--hostname localhost --databaseList test --tableList test.test2"
```

### 3. 其它示例

`flink-demo` 里另有 `WordCount`、`Sideout`、`SQLTest`、`SQLTest1`、`IncrementMapFunction`、
`MockSourceFunction`（埋点日志数据生成器）等，直接在 IDE 里运行 `main` 即可。

---

## 五、测试与验收

### 5.1 三层测试

| 层级 | 内容 | 需要 Docker | 运行方式 |
|---|---|---|---|
| **单元测试** | 类型映射、DDL 生成、参数解析、表名归一化 | 否 | 见下 |
| **解序列化测试** | 用真实 Debezium `SourceRecord` 驱动 `CustomDebeziumDeserializer`，断言 INSERT/UPDATE/DELETE/快照的 `RowKind` 与字段值 | 否 | 见下 |
| **端到端测试** | 真实 MySQL 容器 + 真实 Flink 作业，验证快照与 binlog 变更 | **是** | 见下 |

```bash
# 默认：跑 20 个不需要 Docker 的测试 + spotless 检查 + 打包
./mvnw -Pflink-1.20 verify
./mvnw -Pflink-2.2 -Pflink2 -Pflink2-test-guava verify

# 端到端（需要 Docker）
./mvnw -Pflink-1.20 -pl sync_database_mysql -am test -Ddocker.tests=true
./mvnw -Pflink-2.2 -Pflink2 -Pflink2-test-guava -pl sync_database_mysql -am test -Ddocker.tests=true
```

### 5.2 端到端测试覆盖了什么

`MysqlCdcEndToEndIT` 会：

1. 启动带 binlog 的 MySQL 8.0 容器（`binlog_format=row`、`binlog_row_image=full`）
2. 预置两行数据，启动**真实的 CDC 作业**，断言快照阶段以 `INSERT` 输出
3. 在作业运行中执行 `INSERT` / `UPDATE` / `DELETE`
4. 断言从 **binlog** 捕获到 4 个事件：`INSERT` → `UPDATE_BEFORE` → `UPDATE_AFTER` → `DELETE`，
   且字段值（含 update 前后的 qty）正确

这是唯一能证明"CDC 链路真的能跑"的测试。单元测试只能证明函数正确，
无法发现序列化、权限、时区、版本冲突这类只在真实作业中出现的问题。

### 5.3 端到端测试实测暴露的真实缺陷

接入后立刻发现一批此前所有测试都发现不了的问题（详见 `SCAN_REPORT.md`）：

- `CustomDebeziumDeserializer` **不可序列化** —— Flink 无法下发算子，任何真实作业提交都会失败
- `flink-connector-base` 被错误固定在 1.17.1，报
  `NoSuchMethodError: SingleThreadFetcherManager.<init>`
- CDC 用户缺少 `REPLICATION CLIENT` 权限，`SHOW MASTER STATUS` 被拒
- 连接器时区与 MySQL 服务器时区不一致，作业反复重启
- Flink 2.2 下 shaded Guava 冲突（`guava31` vs `guava33`）导致增量快照线程起不来

### 5.4 Flink 2.x 的 shaded Guava 冲突（上游未修复）

Flink 2.2 的 Flink core 需要 `flink-shaded-guava 33.4.0`（包前缀 `guava33`），
而 CDC 3.6.0 的 `IncrementalSourceStreamFetcher` 硬引用 `guava31`。
**两者是同一个 Maven 构件**，所以 Maven 只能解析其中一个版本：

```
NoClassDefFoundError: org/apache/flink/shaded/guava33/.../Lists
NoClassDefFoundError: org/apache/flink/shaded/guava31/.../ThreadFactoryBuilder
```

这是上游 bug [FLINK-39429](https://issues.apache.org/jira/browse/FLINK-39429)（状态 **Open**，
修复 PR #4462 已关闭且未合并）。两个 jar 的包树完全不相交（实测 0 个重名类），可以安全共存，
因此本工程用 `-Pflink2-test-guava` profile 把 `guava31` 额外复制并追加到测试 classpath 来绕过。

> 该问题只影响 **Flink 2.x 线**；1.20 线无此冲突。
> 等 FLINK-39429 修复并发布后，可删除 `flink2-test-guava` profile 与模块 POM 中的对应配置。

### 5.5 Docker 环境注意事项

- 若 Docker Hub 不可达，需先本地准备好 `mysql:8.0`，并设置
  `TESTCONTAINERS_RYUK_DISABLED=true`（Ryuk 镜像同样要拉取）。
- Windows 上若报 `Could not find a valid Docker environment`，显式指定
  `DOCKER_HOST=npipe:////./pipe/dockerDesktopLinuxEngine`。

### 5.5 依赖安全治理

工程扫出 **60 个构件命中已知漏洞**，但绝大多数是**假阳性**：它们来自 Flink/Hadoop 的
`provided` 依赖 —— 只用于编译，不进产物，由 Flink 集群在运行时提供。
判断依据是实际打开产物 jar 核对类路径，而不是看依赖树。

真正进 fat jar 且可修复的已全部处理：

| 构件 | 原版本 | 现版本 | 说明 |
|---|---|---|---|
| `org.yaml:snakeyaml` | `1.32` 和 `android:1.23` | **2.4** | `android` 是 javafaker 写错的版本号 |
| `com.fasterxml.jackson.core:jackson-databind` | 2.18.6 | **2.18.11** | 2.18.6 有 7 个漏洞，2.18.11 为 0 |
| `org.apache.commons:commons-compress` | 1.24.0 | **1.27.1** | 传递依赖 |
| `com.nimbusds:nimbus-jose-jwt` | 4.41.1 | **9.37.4** | 传递依赖 |

另外**删除了一处死依赖**：`com.github.javafaker:javafaker` 声明了但全工程无任何代码引用，
而它的 POM 把 snakeyaml 版本写成不存在的 `"android"`，导致
`org.yaml:snakeyaml:jar:android:1.23` 被解析进来并带入 8 个漏洞。
因为 `android` 不是真实版本号，`dependencyManagement` 无法覆盖，只能删除该依赖。

**注意 `log4j 1.2.17` 与 `netty` 是假阳性** —— 实测产物 jar 内
`org/apache/log4j/net/SocketServer.class`、`io/netty/*` 均为 **0 个类**，
说明 shade 插件的排除规则已生效，1.2.17 并未被打包。

#### 持续防护

1. **`maven-enforcer-plugin`**（每次构建执行）：
   - `banCircularDependencies` —— 禁止依赖环
   - `enforceBytecodeVersion`（max JDK 11）—— 防止依赖引入高版本字节码导致
     `UnsupportedClassVersionError`
   - `requireJavaVersion` / `requireMavenVersion` —— 环境不符时快速失败
   > 刻意**未启用** `dependencyConvergence`（本树有 27 处来自 Hadoop 内部 2.10.0 与 3.1.3
   > 并存的良性差异）与 `banDuplicateClasses`（Flink 构件之间按设计就重复打包类，
   > 例如 flink-cdc-runtime 与 flink-table-planner 都内嵌 Calcite），
   > 强行开启只会带来大量无意义抑制项。

2. **OWASP dependency-check**（CI `dependency-scan` job）：
   每次推送扫描依赖树，CVSS ≥ 7 直接失败。抑制项写在
   [`owasp-suppressions.xml`](owasp-suppressions.xml)，**每条都必须写明为什么无法在本仓库修复**。

3. **Dependabot**：每周检查 Maven 与 GitHub Actions 更新，Flink 相关构件分组升级
   （版本必须整体移动，单独升级会破坏构建）。

---

## 六、原始建表语句

```sql
create table if not exists t_user(
	`id` BIGINT NOT NULL,
	`name` VARCHAR(255),
	`age` TINYINT,
	`create_time` TIMESTAMP(0) default '1970-01-01 09:00:00',
	`update_time` TIMESTAMP(0) default '1970-01-01 09:00:00',
PRIMARY KEY (id)
);

create table if not exists test2(
	`id` VARCHAR(200) NOT NULL,
	`student` VARCHAR(200),
	`sex` VARCHAR(200),
	`sexs` VARCHAR(255),
	`op` VARCHAR(255),
	`sc` VARCHAR(255),
	`st` VARCHAR(255),
	`s9` VARCHAR(255),
	`s10` VARCHAR(255),
PRIMARY KEY (id)
);
```

Blog: https://sophiadata.github.io/Bigdata_Blog_Website/

![img](https://user-images.githubusercontent.com/34996528/202855293-c3a35d5b-242b-4e26-848f-a88741cd3afc.png)
