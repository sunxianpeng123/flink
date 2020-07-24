package com.xiaohulu
import java.text.SimpleDateFormat
import java.time.ZoneId
import java.util.{Date,}

import com.google.gson.{Gson, JsonParser}
import com.xiaohulu.bean.{AnchorResultBean, DataBean}
import com.xiaohulu.conf.ConfigTool
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.log4j.{Level, Logger}
import org.apache.flink.streaming.api.scala._
/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/23
  * \* Time: 18:27
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object FlinkScheduleApp {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //获取flink的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //
    //    val fsPath = "file:///checkpoints//"
    val fsPath = "file:///F:\\PythonProjects/checkpoints//fs/"
    val rocksdbPath = "file:///F:\\PythonProjects/checkpoints//rocksdb/"

    //    env.setStateBackend(new MemoryStateBackend())
    //    env.setStateBackend(new FsStateBackend(fsPath, false))
    env.setStateBackend(new RocksDBStateBackend(rocksdbPath,false))
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
    ConfigTool.kafkaProps.setProperty("hdfs.path",ConfigTool.anchor_basic_path)
    val consumer = new FlinkKafkaConsumer[String](ConfigTool.anchor_basic_info_topics, new SimpleStringSchema(), ConfigTool.kafkaProps)
    val sourceDataStream = env.addSource(consumer)
    println("start data process")


    val data = sourceDataStream.map(line=>{
      val anchorBasicMark = "scene_basic"
      val jsonParse = new JsonParser()
      val gs = new Gson()
      val sdf = new SimpleDateFormat("yyyyMMdd")
      var dataArr: Array[DataBean] = Array.empty
      var anchorBasicInfoArr: Array[AnchorResultBean] = Array.empty
      try{
        val je = jsonParse.parse(line)
        if (je.isJsonArray) dataArr = gs.fromJson(je, classOf[Array[DataBean]])
        else dataArr :+= gs.fromJson(je, classOf[DataBean])

        dataArr.foreach(databean=>{
          var type_name = ""
          var platID = ""
          var time = ""
          var itemTime = ""

          if (databean != null & databean.item != null & databean.item.length > 0) {
            type_name = databean.type_name
            platID = databean.sid
            time = databean.time
            var anchorBasicBean: AnchorResultBean = null
            databean.item.foreach(item => {
              itemTime = item.time
              anchorBasicBean = new AnchorResultBean
              if (item != null) {
                //todo 抖音数据
                if (item.typeName.equals(anchorBasicMark)) {
                  //主播基础信息数据
                  anchorBasicBean.platformId = platID;
                  anchorBasicBean.room_id = item.uid;
                  anchorBasicBean.liveId = item.liveId
                  anchorBasicBean.nickname = item.nickname;
                  anchorBasicBean.display_id = item.display_id;

                  anchorBasicBean.secId = item.secId;
                  anchorBasicBean.secret = item.secret;
                  anchorBasicBean.head = item.head;
                  anchorBasicBean.gender = item.gender;
                  anchorBasicBean.introduce = item.introduce;

                  anchorBasicBean.level = item.level;
                  anchorBasicBean.totalViewer = item.totalViewer
                  anchorBasicBean.onlineViewer = item.onlineViewer
                  anchorBasicBean.dySceneValue = item.dySceneValue
                  anchorBasicBean.dyValue = item.dyValue;

                  anchorBasicBean.dyCoinOut = item.dyCoinOut;
                  anchorBasicBean.fansCount = item.fansCount;
                  anchorBasicBean.followCount = item.followCount;
                  anchorBasicBean.location = item.location;
                  anchorBasicBean.title = item.title;

                  anchorBasicBean.cover = item.cover;
                  if (itemTime.equals("")) anchorBasicBean.timestamp = time
                  else anchorBasicBean.timestamp = itemTime
                  val date = sdf.format(new Date(anchorBasicBean.timestamp.toLong * 1000))
                  anchorBasicBean.date = date

                  anchorBasicInfoArr :+= anchorBasicBean
                }
              }
            })

          }
        })
      }catch {
        case e:Exception =>println(e.printStackTrace())
      }
      anchorBasicInfoArr
    })

    data.flatMap(e=>e).print()
    val zone = "Asia/Shanghai"
    val pathFormat = "yyyy-MM-dd"
    val parquetPath = ConfigTool.anchor_basic_path
    val bucketAssigner = new DateTimeBucketAssigner(pathFormat, ZoneId.of(zone))
//    import org.apache.flink.formats.parquet.avro.ParquetAvroWriter
//    val  streamingFileSink = StreamingFileSink.forBulkFormat(new Path(parquetPath), ParquetFileWriter)
//    val  streamingFileSink = StreamingFileSink.forBulkFormat(new Path(parquetPath), ParquetAvroWriters.forReflectRecord(TopicSource.class)).withBucketAssigner(bucketAssigner).build()

    println("down")
    env.execute("Flink Anchor Stream Live Schedule")

  }
}

