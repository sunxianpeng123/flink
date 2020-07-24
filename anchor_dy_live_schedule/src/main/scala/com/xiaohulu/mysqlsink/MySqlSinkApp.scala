package com.xiaohulu.mysqlsink

import java.util.Properties

import com.google.gson.{Gson, JsonParser}
import com.xiaohulu.mysqlsink.bean.WWWBean
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object MySqlSinkApp {

  private val KAFKA_BROKER = "Kafka-01:9092"
  //private val KAFKA_BROKER = "219.129.216.209:9092"
  private val TRANSACTION_GROUP = "com.xhl.flink05sxp"

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    val source = new URI("file:///tmp/checkpoint")
    //    env.setStateBackend(new FsStateBackend(source));
    //    env.enableCheckpointing(1000*60)
    //    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new MemoryStateBackend(1000, false))
    //env.setStateBackend( new RocksDBStateBackend(new URI("hdfs:///192.168.120.160/check_point/")))

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure Kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    val topic = "www_request_log"

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), kafkaProps)
    val transaction = env.addSource(consumer)
    println("start data process")

    val orginDataStream = transaction.flatMap(line => {
      var dbArr: Array[WWWBean] = null
      val jsonParse = new JsonParser()
      val gs = new Gson()
      try {
        val je = jsonParse.parse(line)
        if (je.isJsonArray) {
          dbArr = gs.fromJson(je, classOf[Array[WWWBean]])
        } else {
          val dbb = gs.fromJson(je, classOf[WWWBean])
          dbArr = new Array[WWWBean](1)
          dbArr(0) = dbb
        }
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
      dbArr
    }
    )


    val dataStream = orginDataStream.map(x => {
      x.row_time = x.time
      val msgBean = new MessageBean
      msgBean.platform_id = x.time.toString
      msgBean.room_id =x.time.toString
      msgBean.from_id = x.xhlid
      //msgBean.timestamp =x.time
      msgBean.content=x.xhlid
      //val date  = sdf.format(new Date(x.time.toLong * 1000))
      //msgBean.date = date
      msgBean
    })
    dataStream.addSink(new SinkToMySql)
    println("down")
    env.execute("Flink kafka sink to mysql")
  }


}
