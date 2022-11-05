## MySQL CDC FAQ 参考 本文作了分类处理

https://github.com/ververica/flink-cdc-connectors/wiki/FAQ(ZH)

## Q1: 使用CDC 2.x版本，只能读取全量数据，无法读取增量（binlog） 数据，怎么回事？

CDC 2.0 支持了无锁算法，支持并发读取，为了保证全量数据 + 增量数据的顺序性，依赖Flink 的 checkpoint机制，所以作业需要配置 checkpoint。 SQL 作业中配置方式：

```
Flink SQL> SET 'execution.checkpointing.interval' = '3s';    
```

DataStream 作业配置方式：

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(3000);  
```

## Q2: 使用 MySQL CDC，增量阶段读取出来的 timestamp 字段时区相差8小时，怎么回事呢？

在解析binlog数据中的timestamp字段时，cdc 会使用到作业里配置的server-time-zone信息，也就是MySQL服务器的时区，如果这个时区没有和你的MySQL服务器时区一致，就会出现这个问题。

此外，如果是在DataStream作业中自定义列化器如 MyDeserializer implements DebeziumDeserializationSchema, 自定义的序列化器在解析 timestamp 类型数据时，需要参考下 RowDataDebeziumDeserializeSchema 中对 timestamp 类型的解析，用时给定的时区信息。
```
private TimestampData convertToTimestamp(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Long) {
            switch (schema.name()) {
                case Timestamp.SCHEMA_NAME:
                    return TimestampData.fromEpochMillis((Long) dbzObj);
                case MicroTimestamp.SCHEMA_NAME:
                    long micro = (long) dbzObj;
                    return TimestampData.fromEpochMillis(micro / 1000, (int) (micro % 1000 * 1000));
                case NanoTimestamp.SCHEMA_NAME:
                    long nano = (long) dbzObj;
                    return TimestampData.fromEpochMillis(nano / 1000_000, (int) (nano % 1000_000));
            }
        }
        LocalDateTime localDateTime = TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone);
        return TimestampData.fromLocalDateTime(localDateTime);
    }
 ```
 ## Q3: mysql cdc支持监听从库吗？从库需要如何配置？
 
 支持的，从库需要配置 log-slave-updates = 1 使从实例也能将从主实例同步的数据写入从库的 binlog 文件中，如果主库开启了gtid mode，从库也需要开启。
 
 ```
 log-slave-updates = 1
gtid_mode = on 
enforce_gtid_consistency = on 
```

## Q4: 我想同步分库分表，应该如何配置？

通过 mysql cdc 表的with参数中，表名和库名均支持正则配置，比如 'table-name' ='user_.' 可以匹配表名 user_1, user_2,user_a表，注意正则匹配任意字符是'.' 而不是 '*', 其中点号表示任意字符，星号表示0个或多个，database-name也如此。

## Q5: 我想跳过存量读取阶段，只读取 binlog 数据，怎么配置？

在 mysql cdc 表的 with 参数中指定 'scan.startup.mode' = 'latest-offset' 即可。

## Q6: 我想获取数据库中的 DDL事件，怎么办，有demo吗？

CDC 2.1 版本提供了 DataStream API： MysqlSource， 用户可以配置 includeSchemaChanges 表示是否需要DDL 事件，获取到 DDL 事件后自己写代码处理。

```
 public void consumingAllEvents() throws Exception {
        inventoryDatabase.createAndInitialize();
        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + ".products")
                        .username(inventoryDatabase.getUsername())
                        .password(inventoryDatabase.getPassword())
                        .serverId("5401-5404")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true) // 这里配置，输出DDL事件
                        .build();
				... // 其他处理逻辑                        
    }
```

## Q7: MySQL 整库同步怎么做, Flink CDC 支持吗？

Flink CDC 支持的，Q6 中 提供的 DataStream API 已经可以让用户获取 DDL 变更事件和数据变更事件，用户需要在此基础上，根据自己的业务逻辑和下游存储进行 DataStream 作业开发。

- ## note:
  在 FFA2021 大会上云邪老师展示了团队解决 schema 变更等问题，未来有计划开源。
  
- https://developer.aliyun.com/special/ffa2021/live?spm=a2c6h.12873639.0.0.617e57999nfFsw#

- https://www.bilibili.com/video/BV1tT4y1m7zG?p=2

## Q8: 同一个实例下，某个库的表无法同步增量数据，其他库都可以，这是为啥？

这个问题是因为 mysql 服务器 可以配置 binlog 过滤器，忽略了某些库的 binlog。用户可以通过 show master status 命令查看 Binlog_Ignore_DB 和 Binlog_Do_DB。

```
mysql> show master status;
+------------------+----------+--------------+------------------+----------------------+
| File             | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set    |
+------------------+----------+--------------+------------------+----------------------+
| mysql-bin.000006 |     4594 |              |                  | xxx:1-15             |
+------------------+----------+--------------+------------------+----------------------+
```

## Q9: 作业报错 The connector is trying to read binlog starting at GTIDs xxx and binlog file 'binlog.000064', pos=89887992, skipping 4 events plus 1 rows, but this is no longer available on the server. Reconfigure the connector to use a snapshot when needed，怎么办呢？

出现这种错误是 作业正在读取的binlog文件在 MySQL 服务器已经被清理掉，这种情况一般是 MySQL 服务器上保留的 binlog 文件过期时间太短，可以将该值设置大一点，比如7天。

```
mysql> show variables like 'expire_logs_days';
mysql> set global expire_logs_days=7;
```
还有种情况是 flink cdc 作业消费binlog 太慢，这种一般分配足够的资源即可。

## Q10: 作业报错 ConnectException: A slave with the same server_uuid/server_id as this slave has connected to the master，怎么办呢？

出现这种错误是 作业里使用的 server id 和其他作业或其他同步工具使用的server id 冲突了，server id 需要全局唯一，server id 是一个int类型整数。 在 CDC 2.x 版本中，source 的每个并发都需要一个server id，建议合理规划好server id，比如作业的 source 设置成了四个并发，可以配置 'server-id' = '5001-5004', 这样每个 source task 就不会冲突了。

## Q11: 作业报错 ConnectException: Received DML ‘…’ for processing, binlog probably contains events generated with statement or mixed based replication format，怎么办呢？

出现这种错误是 MySQL 服务器配置不对，需要检查下 binlog_format 是不是 ROW? 可以通过下面的命令查看

```
mysql> show variables like '%binlog_format%'; 
```

## Q12: 作业报错 Mysql8.0 Public Key Retrieval is not allowed， 怎么办呢 ?

这是因为用户配置的 MySQL 用户 使用的是 sha256 密码认证，需要 TLS 等协议传输密码。一种简单的方法是使允许 MySQL用户 支持原始密码方式访问。

```
mysql> ALTER USER 'username'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password';
mysql> FLUSH PRIVILEGES; 
```

## Q13: 作业报错 EventDataDeserializationException: Failed to deserialize data of EventHeaderV4 .... Caused by: java.net.SocketException: Connection reset， 怎么办呢 ?

这个问题一般是网络原因引起，首先排查flink 集群 到 数据库之间的网络情况，其次可以调大 MySQL 服务器的网络参数。

```
mysql> set global slave_net_timeout = 120; 
mysql> set global thread_pool_idle_timeout = 120;
```

## Q14: 作业报错 The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1, but the master has purged binary logs containing GTIDs that the slave requires. 怎么办呢 ?

出现这个问题的原因是的作业全量阶段读取太慢，在全量阶段读完后，之前记录的全量阶段开始时的 gtid 位点已经被 mysql 清理掉了。这种可以增大 mysql 服务器上 binlog 文件的保存时间，也可以调大 source 的并发，让全量阶段读取更快。

## Q15: 在 DataStream API中构建MySQL CDC源时如何配置tableList选项？

tableList选项要求表名使用数据库名，而不是DataStream API中的表名。对于MySQL CDC源代码，tableList选项值应该类似于‘my_db.my_table’。


