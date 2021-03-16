package com.xiaohulu.demo.test

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import transform.TransformGoods

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2021/2/26
  * \* Time: 18:46
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * Program arguments -local_path F:\ScalaProjects\flink\flink_study\src\main\scala\com\xiaohulu\demo\test\db_cs.properties
  * ./flink.bat run -m remote_ip:8090 -p 1 -c com.test.TestLocal ../flink-streaming-report-forms-1.0-SNAPSHOT-jar-with-dependencies.jar -local_path path
  * \*/
object FlinkGoodsApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val parameters = ParameterTool.fromArgs(args)
    val local_path = parameters.get("local_path", null) //指定参数名：local_path
    println(s"local path =$local_path")
    //读取配置文件
    val paramFromProps = ParameterTool.fromPropertiesFile(local_path)

    val windowSize = paramFromProps.get("windowSize").toInt
    val slidingSize = paramFromProps.get("slidingSize").toInt
    val sleepSize = paramFromProps.get("sleepSize").toInt

    val jdbcUrl = paramFromProps.get("jdbcUrl")
    val username = paramFromProps.get("username")
    val password = paramFromProps.get("password")


    val anchor_goods_topics = paramFromProps.get("anchor_goods_topics")
    val brokers = paramFromProps.get("brokers")


    val enableCheckpointing = paramFromProps.get("enableCheckpointing").toInt
    val betweenCheckpoints = paramFromProps.get("betweenCheckpoints").toInt
    val checkpointTimeout = paramFromProps.get("checkpointTimeout").toInt

    // 10分钟 启动一次checkpoint 根据数据容忍度
    env.enableCheckpointing(enableCheckpointing)
    //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) //精确一致
    // 设置两次checkpoint的最小时间间隔 10秒
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(betweenCheckpoints)
    // 允许的最大checkpoint并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // checkpoint超时的时长 1分钟
    env.getCheckpointConfig.setCheckpointTimeout(checkpointTimeout)
    // 当程序关闭的时，触发额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //env.enableCheckpointing(300000, CheckpointingMode.EXACTLY_ONCE, force = true)

    //val path = "file:///E:/work/xq/flink/checkpoint/one"
    //val path = "file:///root/test/checkpoint"
    //val rocksBackend = new RocksDBStateBackend(path, true)  //设置增量ck
    //val rocksBackend = new RocksDBStateBackend(path)

    //env.setStateBackend(rocksBackend)
    //env.setStateBackend(new FsStateBackend(path))


    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, bsSettings)

    val properties = new Properties()
    //properties.setProperty("bootstrap.servers", PropertyUtil.getString("brokers"))
    properties.setProperty("bootstrap.servers", brokers)
    properties.setProperty("group.id", "test200")

    //val topic = PropertyUtil.getString("anchor_goods_topics")
    //val kafkaSource = new FlinkKafkaConsumer[String]("crawler-data-scene-goods-infos-71", new SimpleStringSchema(), properties)
    val kafkaSource = new FlinkKafkaConsumer[String](anchor_goods_topics, new SimpleStringSchema(), properties)

    //指定kafka的消费 位点
    //kafkaSource.setStartFromEarliest()
    kafkaSource.setStartFromLatest()

    // 默认行为，从上次消费的偏移量进行继续消费,必须配置group.id参数
    //kafkaSource.setStartFromGroupOffsets()

    /**
      * Flink从topic中指定的时间点开始消费，指定时间点之前的数据忽略
      *
      * 设置开始时间戳 2020-07-28 00:00:00
      */
    //kafkaSource.setStartFromTimestamp(1596529536 * 1000L)
    val input = env.addSource(kafkaSource).uid("s1")
    //println("=== 并行度 === "+input.getParallelism)

    val arrStream = TransformGoods.sourceTrans(input)
    arrStream.flatMap(e => e).print()


    env.execute()

  }
}

