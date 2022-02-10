## Q1 : Debezium for MySQL 的简单示例
```
 curl -H "Content-Type: application/json" -X POST -d  '{
      "name" : "xxxx_2022",
      "config" : {
          "connector.class" : "io.debezium.connector.mysql.MySqlConnector",
          "database.hostname" : "xxx.xxx.xx.xxx",
          "database.port" : "3306",
          "database.user" : "user",
          "database.password" : "pass",
          "database.server.id" : "122811",
          "database.server.name" : "xxxx_2022",
          "database.include.list" : "xxxx",
          "snapshot.mode" : "schema_only",
          "database.history.skip.unparseable.ddl": "true",
          "key.converter": "org.apache.kafka.connect.json.JsonConverter",
          "value.converter": "org.apache.kafka.connect.json.JsonConverter",
          "key.converter.schemas.enable": "false",
          "value.converter.schemas.enable": "false",
          "include.schema.changes": "true",
          "decimal.handling.mode" : "string",
          "transforms": "unwrap",
          "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
          "transforms.unwrap.drop.tombstones": "true",
          "transforms.unwrap.delete.handling.mode": "rewrite",
          "transforms.unwrap.add.fields": "source.ts_ms,source.db,source.table,op",
          "converters" : "datetime",
          "datetime.type" : "com.darcytech.debezium.converter.MySqlDateTimeConverter",
          "datetime.format.date" : "yyyy-MM-dd",
          "datetime.format.time" : "HH:mm:ss",
          "datetime.format.datetime" : "yyyy-MM-dd HH:mm:ss",
          "datetime.format.timestamp" : "yyyy-MM-dd HH:mm:ss",
          "datetime.format.timestamp.zone" : "UTC+8",
          "table.include.list" : "xxxx.user,xxxx.ordertrance,xxxx.ordertrance_agent_info",
          "database.history.kafka.bootstrap.servers":"xxxx.xxx.xx.xxx:9092,xxxx.xxx.xx.xxx:9092,xxxx.xxx.xx.xxx:9092",
          "database.history.kafka.topic":"history_xxxx_2022"
      }
  }' http://xxxx.xxx.xx.xxx:8083/connectors 
```
> note

Debezium 转换 MySQL 时间类型比较混乱，需要重新做时间转换，具体可以参考

- https://github.com/holmofy/debezium-datetime-converter/blob/master/src/main/java/com/darcytech/debezium/converter/MySqlDateTimeConverter.java
- https://blog.hufeifei.cn/2021/03/DB/mysql-binlog-parser/index.html
