package com.xiaohulu.streaming.windowed.timewindow

import java.time.Duration
import java.util.Properties

import com.xiaohulu.streaming.windowed.timewindow.bean.AnchorResultBean
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.log4j.{Level, Logger}

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/6/7
  * \* Time: 18:16
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object KafkaWatemark {
  val where_to_run = "online"
  val anchor_basic_info_topics = "crawler-data-scene-basic-71"
  val brokers = "Kafka-01:9092"
  val groupId = "kaifa_dy_live_20200617"
  val kafkaProps = new Properties()
  kafkaProps.setProperty("bootstrap.servers", brokers)
  kafkaProps.setProperty("group.id", groupId)

  def main(args: Array[String]): Unit = {
    /**
      * 固定延迟生成水印策略，一个延迟3秒的固定延迟水印，
      */
    //  获取运行环境
    Logger.getLogger("org").setLevel(Level.ERROR)
    //获取flink的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    if (where_to_run.equals("local")) env.getConfig.setAutoWatermarkInterval(500) else env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val consumer = new FlinkKafkaConsumer[String](anchor_basic_info_topics, new SimpleStringSchema(), kafkaProps)
    //当使用Apache Kafka作为数据源时，每个Kafka分区可能都有一个简单的事件时间模式(升序时间戳或有界的外部长度)。然而，当使用来自Kafka的流时，
    //常常会并行使用多个分区，交叉使用来自分区的事件并破坏每个分区的模式(这是Kafka客户端工作的固有方式)。
    //例如，如果每个Kafka分区的事件时间戳是严格升序的，那么使用升序时间戳水印生成器生成每个分区的水印将得到完美的整体水印。
    // 注意，在示例中我们没有提供TimestampAssigner，而是使用Kafka记录本身的时间戳。
    //下面的插图展示了如何使用每个kafka分区的水印生成，以及在这种情况下水印是如何通过流数据流传播的。
    consumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[String](Duration.ofSeconds(20)))
    val kafkaSource = env.addSource(consumer)
    val stream = FlinkStreamMap.analysisDyAnchorKafkaStream(kafkaSource)
    stream.print()

    env.execute("FlinkSource01")
  }
}



