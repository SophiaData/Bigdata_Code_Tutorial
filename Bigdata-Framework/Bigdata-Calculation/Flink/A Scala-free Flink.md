# A Scala-free Flink

A detailed [blog post](https://flink.apache.org/2022/02/22/scala-free.html)

already explains the ins and outs of why Scala users can now use the Flink Java API with any Scala version (including Scala 3).

In the end, removing Scala is just part of a larger effort of cleaning up and updating various technologies from the Flink ecosystem.

## 博文翻译

### 类路径和 Scala

如果您使用过基于 JVM 的应用程序，您可能听说过术语类路径。

类路径定义了当需要加载给定类文件时 JVM 将在哪里搜索它。

每个类路径上可能只有一个类文件的实例，这迫使 Flink 将任何依赖项暴露给用户。

这就是为什么 Flink 社区努力保持我们的类路径“干净”——或者没有不必要的依赖。

我们通过组合阴影依赖、子类加载和可选组件的插件抽象来实现这一点。

Apache Flink 运行时主要是用 Java 编写的，但包含强制 Scala 使用默认类路径的关键组件。

而且由于 Scala 不保持跨次要版本的二进制兼容性，这在历史上需要为所有版本的 Scala 交叉构建组件。

但是由于许多原因——编译器的重大变化、新的标准库和重新设计的宏系统——说起来容易做起来难。

### 隐藏 Scala

如上所述，Flink 在几个关键组件中使用了 Scala；Mesos 集成、序列化堆栈、RPC 和表规划器。社区没有删除这些依赖项或寻找交叉构建它们的方法，而是隐藏了 Scala。

它仍然存在于代码库中，但不再泄漏到用户代码类加载器中。

在 1.14 中，我们迈出了向用户隐藏 Scala 的第一步。我们放弃了对部分在 Scala 中实现的 Apache Mesos 的支持，Kubernetes 在采用方面远远超过了它。

接下来，我们将 RPC 系统隔离到一个专用的类加载器中，包括 Akka。通过这些更改，运行时本身不再依赖于 Scala（这就是为什么 flink-runtime 失去了它的 Scala 后缀），但 Scala 仍然存在于 API 层中。

这些变化，以及我们实施它们的容易程度，开始让人们想知道还有什么可能。

毕竟，我们在不到一个月的时间内就隔离了 Akka，这个任务积压了多年，被认为太耗时了。

下一个合乎逻辑的步骤是将 DataStream / DataSet Java API 与 Scala 分离。

这主要需要对一些测试 类进行少量清理，还需要识别仅与 Scala API 相关的代码路径。然后将这些路径迁移到 Scala API 模块中，并且仅在需要时使用。

例如，我们一直扩展以支持某些 Scala 类型的Kryo 序列化程序现在仅在应用程序使用 Scala API 时才包含它们。

最后，是时候处理 Table API，特别是 table planner，在撰写本文时它包含 378,655 行 Scala 代码。

表规划器将 SQL 和表 API 查询解析、规划和优化为高度优化的 Java 代码。它是 Flink 中最广泛的 Scala 代码库，不能轻易移植到 Java。

使用我们从为 RPC 堆栈构建专用类加载器和为序列化器的条件类加载中学到的知识，我们将规划器隐藏在一个不暴露其任何内部结构的抽象背后，包括 Scala。

### Apache Flink 中 Scala 的未来

虽然这些更改大部分发生在幕后，但它们导致了一个非常面向用户的更改：删除了许多 scala 后缀。您可以在本文结尾处找到丢失 Scala 后缀的所有依赖项的列表。

此外，对 Table API 的更改需要对打包和分发进行一些更改，一些依赖规划器内部的高级用户可能需要适应。

展望未来，Flink 将继续支持针对 Scala 2.12 编译的 DataStream 和 Table API 的 Scala 包，而 Java API 现在已解锁，用户可以利用任何 Scala 版本的组件。我们已经看到社区中出现了新的 Scala 3 包装器，我们很高兴看到用户如何在他们的流媒体管道中利用这些工具！

flink-cep, flink-clients, flink-connector-elasticsearch-base, flink-connector-elasticsearch6, flink-connector-elasticsearch7, flink-connector-gcp-pubsub, flink-connector-hbase-1.4, flink-connector-hbase-2.2, flink-connector-hbase-base, flink-connector-jdbc, flink-connector-kafka, flink-connector-kinesis, flink-connector-nifi, flink-connector-pulsar, flink-connector-rabbitmq, flink-connector-testing, flink-connector-twitter, flink-connector-wikiedits, flink-container, flink-dstl-dfs, flink-gelly, flink-hadoop-bulk, flink-kubernetes, flink-runtime-web, flink-sql-connector-elasticsearch6, flink-sql-connector-elasticsearch7, flink-sql-connector-hbase-1.4, flink-sql-connector-hbase-2.2, flink-sql-connector-kafka, flink-sql-connector-kinesis, flink-sql-connector-rabbitmq, flink-state-processor-api, flink-statebackend-rocksdb, flink-streaming-java, flink-table-api-java-bridge, flink-test-utils, flink-yarn, flink-table-runtime, flink-table-api-java-bridge 

https://nightlies.apache.org/flink/flink-docs-master/docs/dev/configuration/overview/#which-dependencies-do-you-need 

https://nightlies.apache.org/flink/flink-docs-master/docs/dev/configuration/advanced/#anatomy-of-table-dependencies 

https://github.com/ariskk/flink4s 

https://github.com/findify/flink-adt 

https://github.com/sjwiesman/flink-scala-3 

> 根据博文和 flink 1.15 发布公告，目前 flink table planner 对 Scala 的隐藏已经完成，为了向后兼容，1.15 仍然提供了带有 Scala 后缀的 Flink Table Planner，同时提供了重新构建的
> Flink Table Planner Loader


<img width="885" alt="image" src="https://user-images.githubusercontent.com/34996528/167306854-01764dde-4cce-4b24-ab91-b9097c4c774b.png">

[重组table模块，引入flink-table-planner-loader](https://issues.apache.org/jira/browse/FLINK-25128)

[[FLINK-24427] Hiding scala in flink-table-planner.google doc](https://docs.google.com/document/d/12yDUCnvcwU2mODBKTHQ1xhfOq1ujYUrXltiN_rbhT34/edit#)

- note 使用 flink sql-client 时，如果使用 flink-table-planner_2.12 带有 Scala 后缀的 jar 包来使用 flink Scala app 时，需将其放入 opt 目录下，如果放置在 lib 目录下会目前会导致异常

<img width="1092" alt="image" src="https://user-images.githubusercontent.com/34996528/167307335-6161a7a8-9754-4c39-88e5-d1c85eb8c1dc.png">




