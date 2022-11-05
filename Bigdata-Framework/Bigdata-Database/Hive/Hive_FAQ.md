## Q1: Hive 如何使用达梦数据库存储元数据信息？

<img width="476" alt="image" src="https://user-images.githubusercontent.com/34996528/167798676-40494b35-f3c6-4b70-8aea-3cbb15238458.png">

hive-site.xml里面的配置信息全都写成达梦的，然后驱动包也用达梦的，初始化的时候元数据库换成 oracle,schematool -dbType oracle -initSchema
