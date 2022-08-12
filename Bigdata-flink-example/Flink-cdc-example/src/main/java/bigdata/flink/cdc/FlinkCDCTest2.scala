package bigdata.flink.cdc

import bigdata.flink.cdc.config.Config.Config
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.connectors.shaded.org.apache.kafka.clients.producer.ProducerRecord
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}

import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * @author gtk
 * @date 2022/8/12 10:38
 */
object FlinkCDCTest2 {

  def createCDCSource(params:Config): MySqlSource[String]={
    var startPos=StartupOptions.initial()
    if (params.position == "latest"){
      startPos= StartupOptions.latest()
    }

    val prop = new Properties()
    prop.setProperty("decimal.handling.mode","string")
    MySqlSource.builder[String]
      .hostname(params.host.split(":")(0))
      .port(params.host.split(":")(1).toInt)
      .username(params.username)
      .password(params.pwd)
      .databaseList(params.dbList)
      .tableList(params.tbList)
      .startupOptions(startPos)
      .serverId(params.serverId)
      .debeziumProperties(prop)
      .deserializer(new JsonDebeziumDeserializationSchema).build

  }



  def createKafkaSink(params:Config)={
    val kafkaProducerProperties = Map(
      "bootstrap.servers" -> params.brokerList,
      "transaction.timeout.ms"-> "300000"
    )
    val serializationSchema = new KafkaSerializationSchema[String] {
      override def serialize(element: String,
                             timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] =
        new ProducerRecord[Array[Byte], Array[Byte]](
          params.sinkTopic,
          element.getBytes(StandardCharsets.UTF_8))
    }
    new FlinkKafkaProducer[String](
      params.sinkTopic,
      serializationSchema,
      kafkaProducerProperties,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE
    )
  }

  implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
    (new java.util.Properties /: map) { case (props, (k, v)) => props.put(k, v); props }
  }

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = Config.parseConfig(FlinkCDCTest2, args)
    env.enableCheckpointing(params.checkpointInterval.toInt * 1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    val rocksBackend: StateBackend = new RocksDBStateBackend(params.checkpointDir)
    env.setStateBackend(rocksBackend)

    env.fromSource(createCDCSource(params), WatermarkStrategy.noWatermarks(), "mysql cdc source")
      .addSink(createKafkaSink(params)).name("cdc sink msk")
      .setParallelism(params.parallel.toInt)

    env.execute("MySQL Binlog CDC")
  }

}
