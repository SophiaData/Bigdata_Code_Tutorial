整库同步代码

https://github.com/SophiaData/Bigdata_Code_Tutorial/blob/master/sync_database_mysql/src/main/java/io/sophiadata/flink/sync/FlinkSqlWDS.java

程序运行效果图

![img](https://img-blog.csdnimg.cn/92a0a25d2ef94e4a96c791866f723c5c.png)

运行方法，根据需要修改表名，库名，账号，密码，并将 provided 添加到 IDE 运行环境中，
将 log4j.properties 日志级别输出调整为 info
可以得到 flink web ui 地址

根据需要调整 ParameterUtil 里面的参数，也可以使用 nacos 来获取相应参数

Blog: https://sophiadata.github.io/Bigdata_Blog_Website/

![img](https://user-images.githubusercontent.com/34996528/202855293-c3a35d5b-242b-4e26-848f-a88741cd3afc.png)
