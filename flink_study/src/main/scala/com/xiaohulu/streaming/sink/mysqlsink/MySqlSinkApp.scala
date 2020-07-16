package com.xiaohulu.streaming.sink.mysqlsink

import java.text.SimpleDateFormat
import java.util.Properties

import com.google.gson.{Gson, JsonParser}
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
object MySqlSinkApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    获取默认并行度
    val defaultParallelism = env.getParallelism
    println(defaultParallelism)

    val props = new Properties()
    props.put("bootstrap.servers", "Kafka-01:9092")
    props.put("zookeeper.connect", "localhost:2181")
    props.put("group.id", "flink_test_sxp")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest") //value 反序列化

    val myConsumer = new FlinkKafkaConsumer08[String]("node-bullet-crawler-59", new SimpleStringSchema(),props)
    //默认读取上次保存的offset信息
    myConsumer.setStartFromGroupOffsets()
    // 从最早的数据开始进行消费，忽略存储的offset信息
    myConsumer.setStartFromEarliest()
    // 从最新的数据进行消费，忽略存储的offset信息
    myConsumer.setStartFromLatest()
    val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
    //从指定位置进行消费
    specificStartOffsets.put(new KafkaTopicPartition("node-bullet-crawler-59", 0), 23L)
    myConsumer.setStartFromSpecificOffsets(specificStartOffsets)

    val dataStream = env.addSource(myConsumer)

    val dataStreamMaped= dataStream.flatMap(line=>{
      val jsonParse = new JsonParser()
      val gs =new Gson()
      val  sdf = new SimpleDateFormat("yyyyMMdd")
      var msg:scala.Array[Array[MessageBean]]=null
      var num = 0
      try {
        //      println(line)
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
            num += 1
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
                  //msgBean.timestamp =x.time
                  msgBean.content=x.content
                  //val date  = sdf.format(new Date(x.time.toLong * 1000))
                  //msgBean.date = date
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
      //      println(s"rdd :::all = $num")
      msg
    }).flatMap(e=>e)

    dataStreamMaped.addSink(new SinkToMySql)
    env.execute("Flink kafka sink to mysql")
  }
}
