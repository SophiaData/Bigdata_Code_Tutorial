## MongoDB CDC FAQ 参考 本文作了分类处理

https://github.com/ververica/flink-cdc-connectors/wiki/FAQ(ZH)

## Q1: MongoDB CDC 支持 全量+增量读 和 只读增量吗？

支持，默认为 全量+增量 读取；使用copy.existing=false参数设置为只读增量。

## Q2: MongoDB CDC 支持从 checkpoint 恢复吗? 原理是怎么样的呢？

支持，checkpoint 会记录 ChangeStream 的 resumeToken，恢复的时候可以通过resumeToken重新恢复ChangeStream。其中 resumeToken 对应 oplog.rs (MongoDB 变更日志collection) 的位置，oplog.rs 是一个固定容量的 collection。当 resumeToken 对应的记录在 oplog.rs 中不存在的时候，可能会出现 Invalid resumeToken 的异常。这种情况，在使用时可以设置合适oplog.rs的集合大小，避免oplog.rs保留时间过短，可以参考 https://docs.mongodb.com/manual/tutorial/change-oplog-size/ 另外，resumeToken 可以通过新到的变更记录和 heartbeat 记录来刷新。

## Q3: MongoDB CDC 支持输出 -U（update_before，更新前镜像值）消息吗？

MongoDB 原始的 oplog.rs 只有 INSERT, UPDATE, REPLACE, DELETE 这几种操作类型，没有保留更新前的信息，不能输出-U 消息，在 Flink 中只能实现 UPSERT 语义。在使用MongoDBTableSource 时，Flink planner 会自动进行 ChangelogNormalize 优化，补齐缺失的 -U 消息，输出完整的 +I, -U， +U， -D 四种消息， 代价是 ChangelogNormalize 优化的代价是该节点会保存之前所有 key 的状态。所以，如果是 DataStream 作业直接使用 MongoDBSource，如果没有 Flink planner 的优化，将不会自动进行 ChangelogNormalize，所以不能直接获取 —U 消息。想要获取更新前镜像值，需要自己管理状态，如果不希望自己管理状态，可以将 MongoDBTableSource 转换为 ChangelogStream 或者 RetractStream，借助 Flink planner 的优化能力补齐更新前镜像值，示例如下：

```
    tEnv.executeSql("CREATE TABLE orders ( ... ) WITH ( 'connector'='mongodb-cdc',... )");

    Table table = tEnv.from("orders")
            .select($("*"));

    tEnv.toChangelogStream(table)
            .print()
            .setParallelism(1);

    env.execute();
```

## Q4: MongoDB CDC 支持订阅多个 collection 吗？

仅支持订阅整库的 collection，筛选部分 collection 功能还不支持，例如配置 database 为 'mgdb'，collection 为空字符串，则会订阅 'mgdb' 库下所有 collection。

## Q5: MongoDB CDC 支持设置多并发度读取吗？

目前还不支持。

## Q6: MongoDB CDC 支持 MongoDB 的版本是哪些？

MongoDB CDC 基于 ChangeStream 特性实现，ChangeStream 是 MongoDB 3.6 推出的新特性。MongoDB CDC 理论上支持 3.6 以上版本，建议运行版本 >= 4.0, 在低于3.6版本执行时，会出现错误: Unrecognized pipeline stage name: '$changeStream' 。

## Q7: MongoDB CDC 支持 MongoDB 的运行模式是什么？

ChangeStream 需要 MongoDB 以副本集或者分片模式运行，本地测试可以使用单机版副本集 rs.initiate() 。在 standalone 模式下会出现错误：The $changestage is only supported on replica sets.

## Q8: MongoDB CDC 报错用户名密码错误, 但其他组件使用该用户名密码都能正常连接，这是什么原因？

如果用户是创建在需要连接的db 下，需要在with参数里加下 'connection.options' = 'authSource=用户所在的db'。

## Q9: MongoDB CDC 是否支持 debezium 相关的参数？

不支持的，因为 MongoDB CDC 连接器是在 Flink CDC 项目中独立开发，并不依赖Debezium项目，所以不支持。

## Q10：MongoDB CDC 全量读取阶段，作业失败后，可以从 checkpoint 继续读取吗？

MongoDB CDC 全量读取阶段是不做 checkpoint 的，直到全量阶段读取完后才开始作 checkpoint，如果在全量读取阶段失败，MongoDB CDC 会重新读取存量数据。
