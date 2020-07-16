package com.xiaohulu.streaming.windowed.timewindow

import java.text.SimpleDateFormat
import java.util.Properties

import com.google.gson.{Gson, JsonParser}
import com.xiaohulu.bean.{DateBean, MessageBean}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/9/4
  * \* Time: 11:20
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
object FlinkWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.put("bootstrap.servers", "Kafka-01:9092")
    props.put("zookeeper.connect", "localhost:2181")
    props.put("group.id", "flink_test_sxp")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest") //value 反序列化
    val myConsumer = new FlinkKafkaConsumer08[String]("node-bullet-crawler-59", new SimpleStringSchema(),props)
    myConsumer.setStartFromEarliest()      // start from the earliest record possible
    myConsumer.setStartFromLatest()        // start from the latest record
    val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
    specificStartOffsets.put(new KafkaTopicPartition("node-bullet-crawler-15", 0), 23L)
    myConsumer.setStartFromSpecificOffsets(specificStartOffsets)
    println("1111111111")
    val dataStream = env.addSource(myConsumer)
    val dataStreamMaped= dataStream.flatMap(line=>{
      val jsonParse = new JsonParser()
      val gs =new Gson()
      val  sdf = new SimpleDateFormat("yyyyMMdd")
      var msg:scala.Array[Array[MessageBean]]=null
      try {
        println(line)
        val je=jsonParse.parse(line)
        var dbArr:Array[DateBean]=null
        if(je.isJsonArray){
          dbArr= gs.fromJson(je,classOf[Array[DateBean]])
        }else{
          val dbb= gs.fromJson(je,classOf[DateBean])
          dbArr= new Array[DateBean](1)
          dbArr(0)=dbb
        }
        msg = dbArr.map(m=>{
          var platID=""
          var roomID=""
          var msgArr:Array[MessageBean]=Array.empty
          if(m!=null & m.item!=null & m.item.length>0){
            platID=m.sid
            roomID=m.roomid
            var msgBean :MessageBean = null
            m.item.foreach(x=> {
              msgBean = new MessageBean
              if(x!=null){
                if(x.typeName.equals("chat")){
                  msgBean.platform_id =platID
                  msgBean.room_id =roomID
                  msgBean.from_id = x.fromid
                  msgBean.content=x.content
                  msgArr :+= msgBean
                }
              }
            })
          }
          msgArr
        })
      } catch {
        case e: Exception =>println(e.getMessage)
      }
      msg
    }).flatMap(e=>e).map(e=>(e.room_id,1L))
    val dataStreamKeyByed = dataStreamMaped.keyBy(1)
//    Time Windows 根据时间来聚合流数据。
//    dataStreamKeyByed.timeWindow(Time.seconds(1)).sum("msg_count")//tumbling time window 每秒钟统计一次数量和
//    dataStreamKeyByed .timeWindow(Time.seconds(3), Time.seconds(1)).sum(1) //sliding time window 每隔 1s 统计过去3秒的数量和
//    dataStreamKeyByed.countWindow(100).sum(1) //tumbling time window //统计每 100 个元素的数量之和
    dataStreamKeyByed.countWindow(100,10).sum(1) //每 10 个元素统计过去 100 个元素的数量之和

    dataStreamKeyByed.print()
    env.execute("Flink window ")
  }
}
