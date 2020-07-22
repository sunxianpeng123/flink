//package com.xiaohulu.streaming.datasource
//
//import java.util.Properties
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011}
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: sunxianpeng
//  * \* Date: 2019/9/4
//  * \* Time: 11:20
//  * \* To change this template use File | Settings | File Templates.
//  * \* Description:
//  * \*/
//object FlinkKafkaTest {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val props = new Properties()
//    props.put("bootstrap.servers", "Kafka-01:9092")
//    props.put("zookeeper.connect", "localhost:2181")
//    props.put("group.id", "flink_test_sxp")
//    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化
//    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    props.put("auto.offset.reset", "latest") //value 反序列化
//    val topic = "crawler-data-scene-basic-71"
////    val topic = "node-bullet-crawler-15"
//    val myConsumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(),props)
////    myConsumer.setStartFromEarliest()      // start from the earliest record possible
////    myConsumer.setStartFromLatest()        // start from the latest record
////    val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
////    specificStartOffsets.put(new KafkaTopicPartition("node-bullet-crawler-15", 0), 23L)
////    myConsumer.setStartFromSpecificOffsets(specificStartOffsets)
//
//    val stream = env.addSource(myConsumer)
//      stream.print()
//
////    val dataStreamSource = env.addSource(new FlinkKafkaConsumer011[String,String]("node-bullet-crawler-15"))
//
//    env.execute("Flink kafka test")
//  }
//}
