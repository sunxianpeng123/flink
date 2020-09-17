package com.xiaohulu.excatly_once

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/9/16
  * \* Time: 15:18
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object Config {
  /**
    * flink 的配置信息
    */
  //设置并行度，为了方便测试，查看消息的顺序，这里设置为1，可以更改为多并行度
  val parallelism = 1
  //每隔10s进行启动一个检查点【设置checkpoint的周期】
  val checkpoint_enable_time = 10000
  //mm毫秒
  //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
  val checkpoint_min_pause_between_time = 1000
  //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
  val checkpoint_timeout_time = 10000
  //同一时间只允许进行一次检查点
  val checkpoint_max_concurrent = 1
  //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
  //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
  //设置statebackend,将检查点保存在hdfs上面，默认保存在内存中。这里先保存到本地
  val checkpoint_statebackend_path = "file:///F:\\ScalaProjects\\flink\\flink_study\\src\\main\\scala\\com\\xiaohulu\\excatly_once/checkpoint/"

  /**
    * Kafka config
    */
   val kafka_broker = "Kafka-01:9092"
  //private val KAFKA_BROKER = "219.129.216.209:9092"
   val kafka_group_id = "com.xhl.flink05sxp"
  val kafka_topic = "www_request_log"
  //kafka分区自动发现周期
  val kafka_key_partition_discovery_interval_millis = "3000"
  /**
    * mysql config
    */
  val dbIpW = "192.168.120.158"
  val dbUserW ="root"
  val dbPasswordW = "1qaz@WSX3edc"
  val dbPassportW = 3306

}

