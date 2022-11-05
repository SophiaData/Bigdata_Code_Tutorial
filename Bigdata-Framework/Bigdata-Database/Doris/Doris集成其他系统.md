## Doris集成其他系统

准备表和数据

```
show full builtin  functions in test_db like 'year';
```
```
CREATE TABLE table1 
(
siteid INT DEFAULT '10', 
citycode SMALLINT,
username VARCHAR(32) DEFAULT '', 
pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, citycode, username) 
DISTRIBUTED BY HASH(siteid) BUCKETS 10 
PROPERTIES("replication_num" = "1");
```
```
insert into table1 values 
(1,1,'jim',2),
(2,1,'grace',2), 
(3,2,'tom',2),
(4,3,'bush',3), 
(5,3,'helen',3);
```
## Spark 读写 Doris

###  准备  Spark 环境

创建 maven 工程，编写 pom.xml 文件
```
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.atguigu.doris</groupId>
    <artifactId>spark-demo</artifactId>
    <version>1.0-SNAPSHOT</version>
    <properties>
        <scala.binary.version>2.12</scala.binary.version>
        <spark.version>3.0.0</spark.version>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
    </properties>
    <dependencies>
        <!-- Spark的依赖引入   -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_${scala.binary.version}</artifactId>
            <scope>provided</scope>
            <version>${spark.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${scala.binary.version}</artifactId>
            <scope>provided</scope>
            <version>${spark.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-hive_${scala.binary.version}</artifactId>
            <scope>provided</scope>
            <version>${spark.version}</version>
        </dependency>
        <!-- 引入 Scala -->
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>2.12.10</version>
        </dependency>
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>1.2.47</version>
        </dependency>
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>5.1.49</version>
        </dependency>
        <!--spark-doris-connector-->
        <dependency>
            <groupId>org.apache.doris</groupId>
            <artifactId>spark-doris-connector-3.1_2.12</artifactId>
            <!--<artifactId>spark-doris-connector-2.3_2.11</artifactId>-->
            <version>1.0.1</version>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <!--编译  scala所需插件-->
            <plugin>
                <groupId>org.scala-tools</groupId>
                <artifactId>maven-scala-plugin</artifactId>
                <version>2.15.1</version>
                <executions>
                    <execution>
                        <id>compile-scala</id>
                        <goals>
                            <goal>add-source</goal>
                            <goal>compile</goal>
                        </goals>
                    </execution>
                    <execution>
                        <id>test-compile-scala</id>
                        <goals>
                            <goal>add-source</goal>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.2</version>
                <executions>
                    <execution>
                        <!-- 声明绑定到 maven的 compile阶段    -->
                        <goals>
                            <goal>compile</goal>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <!-- assembly打包插件   -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>3.0.0</version>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
                <configuration>
                    <archive>
                        <manifest>
                        </manifest>
                    </archive>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                </configuration>
            </plugin>

            <!--<plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-compiler-plugin</artifactId>
            <version>3.6.1</version>
            &lt;!&ndash; 所有的编译都依照  JDK1.8 &ndash;&gt;
            <configuration>
            <source>1.8</source>
            <target>1.8</target>
            </configuration>
            </plugin>-->
        </plugins>
    </build>
</project>
```

### 使用  Spark Doris Connector

Spark Doris Connector 可以支持通过 Spark 读取 Doris 中存储的数据，也支持通过 Spark 写入数据到 Doris。

#### SQL 方式读写数据

```


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
/**
 * TODO
 *
 * @version 1.0
 * @author cjp
 */
object SQLDemo {
  def main( args: Array[String] ): Unit = {
    =
    val sparkConf = new SparkConf().setAppName("SQLDemo")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    sparkSession
    val
    SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession.sql(
      """
        |CREATE TEMPORARY VIEW spark_doris
        |USING doris
        |OPTIONS(
        |  "table.identifier"="test_db.table1",
        |  "fenodes"="hadoop1:8030",
        |  "user"="test",
        |  "password"="test"
        |);
""".stripMargin)
//读取数据
//    sparkSession.sql("select * from spark_doris").show()
    //写入数据
    sparkSession.sql("insert 
      values(99,99,'haha',5)")
  }
}
```

#### DataFrame 方式读写数据（batch）

```


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
/**
 * TODO
 *
 * @version 1.0
 * @author cjp
 */
object DataFrameDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameDemo")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 读取数据
    //    val dorisSparkDF = sparkSession.read.format("doris")
    //      .option("doris.table.identifier", "test_db.table1")
    //      .option("doris.fenodes", "hadoop1:8030")
    //    .option("user", "test")
    //      .option("password", "test")
    //      .load()
    //    dorisSparkDF.show()
    // 写入数据
    import sparkSession.implicits._
    val mockDataDF = List(
      (11, 23, "haha", 8),
      (11, 3, "hehe", 9),
      (11, 3, "heihei", 10)
    ).toDF("siteid", "citycode", "username", "pv")
    mockDataDF.show(5)
    mockDataDF.write.format("doris")
      .option("doris.table.identifier", "test_db.table1")
      .option("doris.fenodes", "hadoop1:8030")
      .option("user", "test")
      .option("password", "test")
      //指定你要写入的字段
      //      .option("doris.write.fields", "user")
      .save()
  }

}
```

#### RDD 方式读取数据

```

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
/**
 * TODO
 *
 * @version 1.0
 * @author cjp
 */
object RDDDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RDDDemo")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sc = new SparkContext(sparkConf)
    import org.apache.doris.spark._
    val dorisSparkRDD = sc.dorisRDD(
      tableIdentifier = Some("test_db.table1"),
      cfg = Some(Map(
        "doris.fenodes" -> "hadoop1:8030",
        "doris.request.auth.user" -> "test",
        "doris.request.auth.password" -> "test"
      ))
    )
    dorisSparkRDD.collect().foreach(println)
  }
}
```

#### 配置和字段类型映射

- 1 通用配置项

<img width="887" alt="image" src="https://user-images.githubusercontent.com/34996528/167132236-a3a95203-7845-4ee0-901d-1675367e7cf2.png">

<img width="890" alt="image" src="https://user-images.githubusercontent.com/34996528/167132163-23461f7a-1fca-462f-a612-91e1993d7aab.png">

<img width="895" alt="image" src="https://user-images.githubusercontent.com/34996528/167132357-47304220-e8f8-4d21-8aab-c00cd4107baf.png">

<img width="904" alt="image" src="https://user-images.githubusercontent.com/34996528/167132534-ed6190d0-cb0f-4f69-b992-e55edfecf099.png">

- 2 SQL 和 Dataframe 专有配置

<img width="883" alt="image" src="https://user-images.githubusercontent.com/34996528/167132622-aea37038-c877-4fa4-81c4-bae5658d8b60.png">

- 3 RDD 专有配置

<img width="921" alt="image" src="https://user-images.githubusercontent.com/34996528/167132684-3a3dcbb1-b1c5-4c0d-a5c9-7ce65f63a08b.png">

- 4 Doris 和 Spark 列类型映射关系:

<img width="900" alt="image" src="https://user-images.githubusercontent.com/34996528/167132772-19c41625-5c50-47cd-97eb-f97df6983431.png">

<img width="876" alt="image" src="https://user-images.githubusercontent.com/34996528/167133133-0f219de7-2410-4e48-b576-892f7a6e5064.png">

<img width="905" alt="image" src="https://user-images.githubusercontent.com/34996528/167132972-38a871bf-2057-4cfa-baf1-37ff494ab00e.png">

### 使用 JDBC 的方式（不推荐）

这种方式是早期写法，Spark 无法感知 Doris 的数据分布，会导致打到 Doris 的查询压力非常大。

```

import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object JDBCDemo {
  def main(args: Array[String]): Unit = {
  val sparkConf =  SparkConf().setAppName("JDBCDemo").setMaster("local[*]")

val  sparkSession =  SparkSession.builder().config(sparkConf).getOrCreate()
// 读取数据
//    val df=sparkSession.read.format("jdbc")
//      .option("url","jdbc:mysql://hadoop1:9030/test_db")
//      .option("user","test")
//      .option("password","test")
//      .option("dbtable","table1")
//      .load()
//
//    df.show()
// 写入数据

import sparkSession.implicits._

   val mockDataDF = List (
      (11, 23, "haha", 8),
      (11, 3, "hehe", 9),
      (11, 3, "heihei", 10)
        ).toDF ("siteid", "citycode", "username", "pv")
     val prop = new Properties ()
     prop.setProperty ("user", "root")
     prop.setProperty ("password", "123456")
     df.write.mode (SaveMode.Append)
       .jdbc ("jdbc:mysql://hadoop1:9030/test_db", "table1", prop)
  }
}
```




