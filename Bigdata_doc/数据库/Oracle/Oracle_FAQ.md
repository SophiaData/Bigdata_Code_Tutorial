## Q1: Oracle 如何实现对同一个表的 upsert 功能？
```
MERGE INTO test T1
USING (select count(*) AS c from test T2 where T2.ID = 'xxxxx211108593445YY2022') x
ON ( x.c > 0 )
WHEN MATCHED THEN
    UPDATE SET USERID = 'asfafsasagsa' where ID = 'xxxxx211108593445YY2021'
WHEN NOT MATCHED THEN
    INSERT VALUES('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21');
 ```
 
 ```
Flink sink 代码风格
             String mergeSql =
                    " MERGE INTO "
                            + sinkTable
                            + " T1 USING (SELECT COUNT (*) AS c FROM"
                            + sinkTable
                            + " T2 WHERE "
                            + StringUtils.join(pkSet, ",")
                            + " = "
                            + StringUtils.join(pkValues, ",")
                            + ") x ON ( x.c > 0 ) WHEN MATCHED THEN UPDATE SET "
                            + " WHEN MATCHED THEN "
                            + getSetSql(value)
                            + " WHERE "
                            + StringUtils.join(pkSet, ",")
                            + " = "
                            + StringUtils.join(pkValues, ",")
                            + "WHEN NOT MATCHED THEN INSERT VALUES ('"
                            + StringUtils.join(values, "','")
                            + "')";
 ```
