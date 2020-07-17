//package com.xiaohulu.streaming.state.checkpointed
//
//import java.util.Properties
//
//import org.apache.flink.api.common.functions.RichFlatMapFunction
//import org.apache.flink.api.common.restartstrategy.RestartStrategies
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.api.scala._
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.core.fs.Path
//import org.apache.flink.runtime.state.StateBackend
//import org.apache.flink.runtime.state.filesystem.FsStateBackend
//import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer08}
//import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
//import org.apache.flink.util.Collector
//import org.slf4j.LoggerFactory
//
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: sunxianpeng
//  * \* Date: 2019/9/4
//  * \* Time: 11:20
//  * \* To change this template use File | Settings | File Templates.
//  * \* Description:
//  * \*/
//object ManagedKeyedStateStreaming {
//
//  private val LOG = LoggerFactory.getLogger(ManagedKeyedStateStreaming.getClass)
//  private val KAFKA_CONSUMER_TOPIC = "crawler-data-scene-basic-71"
//  private val KAFKA_BROKERS = "Kafka-01:9092"
//  private val KAFKA_ZOOKEEPER_CONNECTION="localhost:2181"
//  private val KAFKA_GROUP_ID = "flink_test_sxp"
//  private val auto_offset_reset = "latest"
//  private val KAFKA_PROP: Properties = new Properties() {
//    setProperty("bootstrap.servers", KAFKA_BROKERS)
//    setProperty("zookeeper.connect", KAFKA_ZOOKEEPER_CONNECTION)
//    setProperty("group.id", KAFKA_GROUP_ID)
//    setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") //key 反序列化
//    setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer") //value 反序列化
//    setProperty("auto.offset.reset", auto_offset_reset)
//  }
//
//  def main(args: Array[String]): Unit = {
//    LOG.info("===Stateful Computation Demo===")
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.enableCheckpointing(5000) //5秒一个checkpoint
//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime) //指定处理的时间特性
//    env.setRestartStrategy(RestartStrategies.noRestart()) //重启策略
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) //确保一次语义
//
//    val checkPointPath = new Path("file:///checkpoints/")
//    //fs状态后端配置,如为file:///,则在taskmanager的本地
//    val fsStateBackend: StateBackend = new FsStateBackend(checkPointPath)
//    env.setStateBackend(fsStateBackend)
//    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) //退出不删除checkpoint
//
//
//    val dataStream = env.addSource(new FlinkKafkaConsumer08[String](KAFKA_CONSUMER_TOPIC, new SimpleStringSchema(), KAFKA_PROP))
//    dataStream.print()
//    //    dataStream.filter(_.split("\\|").length==3)
//    //      .map(line=>{
//    //        val arr = line.split("\\|")
//    //        (arr(0),arr(2).toInt)
//    //      }).keyBy(_._1)
//    //      .flatMap(new SalesAmountCalculation())
//    //      .print()
//
//
//    //    val myConsumer = new FlinkKafkaConsumer08[String]("node-bullet-crawler-15", new SimpleStringSchema(),KAFKA_PROP)
//    //    myConsumer.setStartFromEarliest()      // start from the earliest record possible
//    //    myConsumer.setStartFromLatest()        // start from the latest record
//    //    val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
//    //    specificStartOffsets.put(new KafkaTopicPartition("node-bullet-crawler-15", 0), 23L)
//    //    myConsumer.setStartFromSpecificOffsets(specificStartOffsets)
//    //
//    //    val stream = env.addSource(myConsumer)
//    //      stream.print()
//
//    //    val dataStreamSource = env.addSource(new FlinkKafkaConsumer011[String,String]("node-bullet-crawler-15"))
//
//    env.execute("Flink kafka test")
//  }
//}
//
////计算汇总值
//class SalesAmountCalculation extends RichFlatMapFunction[(String, Int), (String, Int)] {
//  private var sum: ValueState[(String, Int)] = _
//
//  override def flatMap(input: (String, Int), out: Collector[(String, Int)]): Unit = {
//    //显式调用已经过期的状态值会被删除，可以配置在读取快照时清除过期状态值，如:
//    //    val ttlConfig = StateTtlConfig
//    //      .newBuilder(Time.seconds(1))
//    //      .cleanupFullSnapshot
//    //      .build
//    val tmpCurrentSum = sum.value
//    val currentSum = if (tmpCurrentSum != null) {
//      tmpCurrentSum
//    } else {
//      (input._1, 0)
//    }
//    val newSum = (currentSum._1, currentSum._2 + input._2)
//    sum.update(newSum)
//    out.collect(newSum)
//  }
//
//  override def open(parameters: Configuration): Unit = {
//    //设置状态值的过期时间
//    //    val ttlConfig = StateTtlConfig
//    //      .newBuilder(Time.seconds(1))//过期时间1秒
//    //      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)//在创建和写入时更新状态值
//    //      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)//过期访问不返回状态值
//    //      .build
//    val valueStateDescriptor = new ValueStateDescriptor[(String, Int)]("sum", createTypeInformation[(String, Int)])
//    //    valueStateDescriptor.enableTimeToLive(ttlConfig)//启用状态值过期配置
//    sum = getRuntimeContext.getState(valueStateDescriptor)
//  }
//}
