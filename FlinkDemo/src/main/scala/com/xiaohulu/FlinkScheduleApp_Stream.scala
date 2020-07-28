package com.xiaohulu

import com.xiaohulu.adapter.KsAdapter
import com.xiaohulu.conf.ConfigTool
import com.xiaohulu.extractor.{DyAnchorExtractor, DyGoodsExtractor}
import com.xiaohulu.transform.FlinkStreamMap
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.log4j.{Level, Logger}


/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/23
  * \* Time: 18:27
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object FlinkScheduleApp_Stream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //获取flink的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val  tEnv = StreamTableEnvironment.create(env)
    //
    //    val fsPath = "file:///checkpoints//"
    val fsPath = "file:///F:\\PythonProjects/checkpoints//fs/"
    val rocksdbPath = "file:///F:\\PythonProjects/checkpoints//rocksdb/"

    //    env.setStateBackend(new MemoryStateBackend())
    //    env.setStateBackend(new FsStateBackend(fsPath, false))
    env.setStateBackend(new RocksDBStateBackend(rocksdbPath, false))
    val config = env.getCheckpointConfig
    //任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
    //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION，表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint处理。
    //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION： 取消作业时保留检查点。请注意，在这种情况下，您必须在取消后手动清理检查点状态。
    //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION： 取消作业时删除检查点。只有在作业失败时，检查点状态才可用。
    //默认情况下，如果设置了Checkpoint选项，则Flink只保留最近成功生成的1个Checkpoint，而当Flink程序失败时，可以从最近的这个Checkpoint来进行恢复。
    // 但是，如果希望保留多个Checkpoint，并能够根据实际需要选择其中一个进行恢复，这样会更加灵活，比如，发现最近4个小时数据记录处理有问题，希望将整个状态还原到4小时之前。
    //Flink可以支持保留多个Checkpoint，需要在Flink的配置文件conf/flink-conf.yaml中，添加如下配置，指定最多需要保存Checkpoint的个数： state.checkpoints.num-retained: 20
    //Flink checkpoint目录分别对应的是 jobId，flink提供了在启动之时通过设置 -s 参数指定checkpoint目录, 让新的jobId 读取该checkpoint元文件信息和状态信息，从而达到指定时间节点启动job。
    config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置checkpoint的周期, 每隔1000 ms进行启动一个检查点
    config.setCheckpointInterval(1000)
    // 设置模式为exactly-once
    config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
    config.setMinPauseBetweenCheckpoints(500)
    // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
    config.setCheckpointTimeout(60000)
    // 同一时间只允许进行一个检查点
    config.setMaxConcurrentCheckpoints(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /** data source */
    //    consumer
    val dyAnchorConsumer = new FlinkKafkaConsumer[String](ConfigTool.anchor_basic_info_topics, new SimpleStringSchema(), ConfigTool.kafkaProps)
    val dyGoodsConsumer = new FlinkKafkaConsumer[String](ConfigTool.anchor_goods_topics, new SimpleStringSchema(), ConfigTool.kafkaProps)
    //    add source
    val dyAnchorSourceDataStream = env.addSource(dyAnchorConsumer)
    val dyGoodsSourceDataStream = env.addSource(dyGoodsConsumer)
    println("start data process")
    /** data transform */
    val dyAnchorDataStream = FlinkStreamMap.analysisDyAnchorKafkaStream(dyAnchorSourceDataStream).assignTimestampsAndWatermarks(new DyAnchorExtractor)
    val dyGoodsDataStream = FlinkStreamMap.analysisDyGoodsKafkaStream(dyGoodsSourceDataStream).assignTimestampsAndWatermarks(new DyGoodsExtractor)
    /**temple table*/
    val dyGoodsWindowStream = dyGoodsDataStream.keyBy(e=>(e.platform_id,e.room_id,e.live_id,e.promotion_id)).timeWindow(Time.seconds(10),Time.seconds(5))
    dyGoodsWindowStream.apply()



//

    /** data sink */
    //sink config
//    val dyAnchorStreamingFileSink = StreamingFileSink.forBulkFormat(new Path(ConfigTool.anchor_basic_path), ParquetAvroWriters.forReflectRecord(classOf[AnchorResultBean])).withBucketAssigner(new DateTimeBucketAssigner[AnchorResultBean]("yyyyMMdd")).build()
//    val dyGoodsStreamingFileSink = StreamingFileSink.forBulkFormat(new Path(ConfigTool.goods_info_path), ParquetAvroWriters.forReflectRecord(classOf[GoodsResultBean])).withBucketAssigner(new DateTimeBucketAssigner[GoodsResultBean]("yyyyMMdd")).build()

    //    add sink
//    anchorRs.toAppendStream[AnchorResultBean].print()//批转流
    println("down")
    env.execute("Flink Anchor Stream Live Schedule")

  }
}

