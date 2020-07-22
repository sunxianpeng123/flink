package com.other

import java.text.SimpleDateFormat
import java.util.Properties

import com.google.gson.{Gson, JsonParser}
import com.other.bean.{DateBean, MessageBean}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object Test {

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
//    val topic = "node-bullet-crawler-59"
    val topic = "www_request_log"

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), kafkaProps)
    val transaction = env.addSource(consumer)
    println("start data process")
transaction.print()
//    val dataStreamMaped = transaction.flatMap(line => {
//      val jsonParse = new JsonParser()
//      val gs = new Gson()
//      val sdf = new SimpleDateFormat("yyyyMMdd")
//      var msg: scala.Array[Array[MessageBean]] = null
//      var num = 0
//      try {
//        //      println(line)
//        val je = jsonParse.parse(line)
//        var dbArr: Array[DateBean] = null
//        if (je.isJsonArray) {
//          dbArr = gs.fromJson(je, classOf[Array[DateBean]])
//        } else {
//          val dbb = gs.fromJson(je, classOf[DateBean])
//          dbArr = new Array[DateBean](1)
//          dbArr(0) = dbb
//        }
//        msg = dbArr.map(m => {
//          var platID = ""
//          var roomID = ""
//          var msgArr: Array[MessageBean] = Array.empty
//          if (m != null & m.item != null & m.item.length > 0) {
//            num += 1
//            platID = m.sid
//            roomID = m.roomid
//            var msgBean: MessageBean = null
//            m.item.foreach(x => {
//              msgBean = new MessageBean
//              if (x != null) {
//                if (x.typeName.equals("chat")) {
//                  msgBean.platform_id = platID
//                  msgBean.room_id = roomID
//                  msgBean.from_id = x.fromid
//                  //msgBean.timestamp =x.time
//                  msgBean.content = x.content
//                  //val date  = sdf.format(new Date(x.time.toLong * 1000))
//                  //msgBean.date = date
//                  msgArr :+= msgBean
//                }
//              }
//            })
//          }
//          msgArr
//        })
//      } catch {
//        case e: Exception => println(e.getMessage)
//      }
//      //      println(s"rdd :::all = $num")
//      msg
//    }).flatMap(e => e)
//    dataStreamMaped.print()
//    dataStreamMaped.addSink(new SinkToMySql)

    println("down")
    env.execute("Flink kafka sink to mysql")
  }


}
