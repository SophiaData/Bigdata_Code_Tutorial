## Q1 : debezium for mysql 的简单示例
```
 curl -H "Content-Type: application/json" -X POST -d  '{
      "name" : "test_xxxx_mysql123012",
      "config" : {
          "connector.class" : "io.debezium.connector.mysql.MySqlConnector",
          "database.hostname" : "10.176.xxx.xx",
          "database.port" : "3306",
          "database.user" : "user",
          "database.password" : "password",
          "database.server.id" : "122811",
          "database.server.name" : "xxxx_mysql123012",
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
          "transforms.unwrap.drop.tombstones": "false",
          "transforms.unwrap.delete.handling.mode": "rewrite",
          "transforms.unwrap.add.fields": "source.ts_ms,source.db,source.table,op",
          "table.include.list" : "xxxx.user,xxxx.ordertrance,xxxx.ordertrance_agent_info",
          "database.history.kafka.bootstrap.servers":"xxxxx05:9092",
          "database.history.kafka.topic":"history_xxxx_mysql123012"
      }
  }' http://xxxxxx05:8083/connectors 
```
