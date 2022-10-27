## Q1: 发现 PG 服务器磁盘使用率高，WAL 不释放 是什么原因？

Flink Postgres CDC 只会在 checkpoint 完成的时候更新 Postgres slot 中的 LSN。因此如果发现磁盘使用率高的情况下，请先确认 checkpoint 是否开启。

## Q2: Flink Postgres CDC 同步 Postgres 中将 超过最大精度（38，18）的 DECIMAL 类型返回 NULL

Flink 中如果收到数据的 precision 大于在 Flink 中声明的类型的 precision 时，会将数据处理成 NULL。此时可以配置相应'debezium.decimal.handling.mode' = 'string' 将读取的数据用 STRING 类型 来处理。

## Q3: Flink Postgres CDC 提示未传输 TOAST 数据，是什么原因？

请先确保 REPLICA IDENTITY 是 FULL。 TOAST 的数据比较大，为了节省 wal 的大小，如果 TOAST 数据没有变更，那么 wal2json plugin 就不会在更新后的数据中带上 toast 数据。为了避免这个问题，可以通过 'debezium.schema.refresh.mode'='columns_diff_exclude_unchanged_toast'来解决。

## Q4: 作业报错 Replication slot "xxxx" is active， 怎么办？

当前 Flink Postgres CDC 在作业退出后并不会手动释放 slot，有两种方式可以解决该问题

- 前往 Postgres 中手动执行以下命令

```
select pg_drop_replication_slot('rep_slot');
    ERROR:  replication slot "rep_slot" is active for PID 162564
select pg_terminate_backend(162564); select pg_drop_replication_slot('rep_slot');
```
- pg source with 参数中添加 'debezium.slot.drop.on.stop' = 'true'，在作业停止后自动清理 slot

## Q5: 作业有脏数据，比如非法的日期，有参数可以配置可以过滤吗？

可以的，可以在 Flink CDC 表的with 参数里 加下 'debezium.event.deserialization.failure.handling.mode'='warn' 参数，跳过脏数据，将脏数据打印到WARN日志里。 也可以配置 'debezium.event.deserialization.failure.handling.mode'='ignore'， 直接跳过脏数据，不打印脏数据到日志。

## Q6: 在DataStream API中构建Postgres CDC源时如何配置tableList选项？

tableList选项要求表名使用架构名，而不是DataStream API中的表名。对于Postgres CDC source，tableList选项值应为‘my_schema.my_table’。
