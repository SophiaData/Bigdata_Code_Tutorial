整库同步代码

https://github.com/SophiaData/Bigdata_Code_Tutorial/blob/master/sync_database_mysql/src/main/java/io/sophiadata/flink/sync/FlinkSqlWDS.java

程序运行效果图

![img](https://img-blog.csdnimg.cn/92a0a25d2ef94e4a96c791866f723c5c.png)

运行方法，根据需要修改表名，库名，账号，密码，并将 provided 添加到 IDE 运行环境中，
将 log4j.properties 日志级别输出调整为 info
可以得到 flink web ui 地址

根据需要调整 ParameterUtil 里面的参数，也可以使用 nacos 来获取相应参数

> 原始建表语句
```sql
create table if not exists jdbc_sink_t_user(
	`id` BIGINT NOT NULL,
	`name` VARCHAR(255),
	`age` TINYINT,
	`create_time` TIMESTAMP(0) NOT NULL default '1970-01-01 09:00:00',
	`update_time` TIMESTAMP(0) NOT NULL default '1970-01-01 09:00:00',
PRIMARY KEY (id)
) 
create table if not exists t_user(
	`id` BIGINT NOT NULL,
	`name` VARCHAR(255),
	`age` TINYINT,
	`create_time` TIMESTAMP(0) default '1970-01-01 09:00:00',
	`update_time` TIMESTAMP(0) default '1970-01-01 09:00:00',
PRIMARY KEY (id)
) 
create table if not exists table_process(
	`source_table` VARCHAR(200) NOT NULL,
	`operate_type` VARCHAR(200) NOT NULL,
	`sink_type` VARCHAR(200),
	`sink_table` VARCHAR(200),
	`sink_columns` VARCHAR(2000),
	`sink_pk` VARCHAR(200),
	`sink_extend` VARCHAR(200),
	`slow_change` VARCHAR(2000),
PRIMARY KEY (source_table,operate_type)
) 
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
) 
create table if not exists test3(
	`id` VARCHAR(200) NOT NULL,
	`student` VARCHAR(200),
	`sex` VARCHAR(200),
	`s11` VARCHAR(200),
PRIMARY KEY (id)
) 
```

Blog: https://sophiadata.github.io/Bigdata_Blog_Website/

![img](https://user-images.githubusercontent.com/34996528/202855293-c3a35d5b-242b-4e26-848f-a88741cd3afc.png)
