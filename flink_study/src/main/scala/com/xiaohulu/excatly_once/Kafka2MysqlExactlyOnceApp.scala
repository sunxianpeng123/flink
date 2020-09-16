package com.xiaohulu.excatly_once

import java.util.Properties

import com.google.gson.{Gson, JsonParser}
import com.xiaohulu.excatly_once.bean.WWWBean
import com.xiaohulu.streaming.sink.mysqlsink.MessageBean
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase}

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/9/16
  * \* Time: 15:10
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 消费kafka消息，sink(自定义)到mysql中，保证kafka to mysql的Exactly-Once
  * \*/
@SuppressWarnings("all")
object Kafka2MysqlExactlyOnceApp {
  import org.apache.flink.streaming.api.scala._
  def main(args: Array[String]): Unit = {
    /**
      * env
      */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度，为了方便测试，查看消息的顺序，这里设置为1，可以更改为多并行度
    env.setParallelism(Config.parallelism)
    //设置模式为：exactly_one，仅一次语义
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 当程序关闭的时，触发额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //checkpoint设置
    //每隔10s进行启动一个检查点【设置checkpoint的周期】
    env.enableCheckpointing(Config.checkpoint_enable_time)
    //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(Config.checkpoint_min_pause_between_time)
    //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
    env.getCheckpointConfig.setCheckpointTimeout(Config.checkpoint_timeout_time)
    //同一时间只允许进行一次检查点
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(Config.checkpoint_max_concurrent)
    //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
    //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    //设置statebackend,将检查点保存在hdfs上面，默认保存在内存中。这里先保存到本地
    env.setStateBackend(new FsStateBackend(Config.checkpoint_statebackend_path))
    /**
      * kafka
      */
    //设置kafka消费参数
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", Config.kafka_broker)
    kafkaProps.setProperty("group.id", Config.kafka_group_id)

    kafkaProps.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, Config.kafka_key_partition_discovery_interval_millis)

    val consumer = new FlinkKafkaConsumer[String](Config.kafka_topic, new SimpleStringSchema(), kafkaProps)
    //加入kafka数据源
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
    })


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
//      msgBean
      (msgBean.platform_id,msgBean.room_id,msgBean.from_id,msgBean.content)
    })
    //数据传输到下游
    dataStream.addSink(new MySqlTwoPhaseCommitSink()).name("MySqlTwoPhaseCommitSink");
    //触发执行
    env.execute(StreamDemoKafka2Mysql.class.getName());

    println("down")
    env.execute("Flink kafka sink to mysql")


  }
}

