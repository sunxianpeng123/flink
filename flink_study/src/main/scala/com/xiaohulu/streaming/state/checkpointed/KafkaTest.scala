//package com.xiaohulu.streaming.state.checkpointed
//
//import java.sql.Timestamp
//import java.util.Properties
//
//import com.google.gson.{Gson, JsonParser}
//import com.xiaohulu.streaming.state.checkpointed.bean.{DataBean, GoodsInfoBean}
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
////import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumer011}
//
//
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: sunxianpeng
//  * \* Date: 2020/7/17
//  * \* Time: 17:01
//  * \* To change this template use File | Settings | File Templates.
//  * \* Description:
//  * \*/
//object KafkaTest {
//  import org.apache.flink.api.scala._
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val props = getKafkaConfig()
//    val topic = "crawler-data-scene-goods-infos-71"
//    print(props)
//    val transaction = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), props))
////    val myConsumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(),props)
//
//    val  dataStream= transaction.flatMap(line => {
//
//      var dbArr: Array[DataBean] = null
//      val jsonParse = new JsonParser()
//      val gs = new Gson()
//      try {
//        val je = jsonParse.parse(line)
//
//        if (je.isJsonArray) {
//          dbArr = gs.fromJson(je, classOf[Array[DataBean]])
//        } else {
//          val dbb = gs.fromJson(je, classOf[DataBean])
//          dbArr = new Array[DataBean](1)
//          dbArr(0) = dbb
//        }
//      }
//      catch {
//        case e: Exception => println(e.getMessage)
//      }
//      dbArr
//    }
//    ).filter(x=>x!=null).flatMap(x=>{x.item}).flatMap(x=>{
//      var goodsInfoList:Array[GoodsInfoBean] = Array.empty
//      x.goodsInfoList.foreach(g=>{
//        g.time=x.time
//        g.room_id=x.uid
//        g.live_id=x.liveId
//        g.row_time=x.time
//        g.eventAccTime= new Timestamp(x.time*1000)
//        goodsInfoList :+= g
//      })
//      print("-----------------------")
//      goodsInfoList
//    })
//      .assignAscendingTimestamps(_.row_time*1000)
//
//    dataStream.print()
//    env.execute()
//
//
//  }
//
//  def getKafkaConfig():Properties={
//    val props = new Properties()
////    第一种
////    props.put("bootstrap.servers", "Kafka-01:9092")
////    props.put("zookeeper.connect", "localhost:2181")
////    props.put("group.id", "flink_test_sxp")
////    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化
////    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
////    props.put("auto.offset.reset", "latest") //value 反序列化
//
////第二种
//     val KAFKA_BROKER = "Kafka-01:9092"
//    //private val KAFKA_BROKER = "219.129.216.209:9092"
//     val TRANSACTION_GROUP = "com.wugenqiang.flink.sxp"
//    props.setProperty("bootstrap.servers", KAFKA_BROKER)
//    props.setProperty("group.id", TRANSACTION_GROUP)
//
//
//
//    props
//  }
//}
//
