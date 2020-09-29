package com.xiaohulu.streaming.state.operatorstate

import java.{lang, util}

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import org.apache.flink.api.scala._
/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/2
  * \* Time: 18:58
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
object ListCheckpointedTest {
  def main(args: Array[String]): Unit = {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val sensorData: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)
    val highTempCnts = sensorData
      .keyBy(_.id)
      .flatMap(new HighTempCounterListCheckPointed(10.0))

    highTempCnts.print()
    env.execute()
  }
}

class HighTempCounterListCheckPointed(val threshold: Double) extends RichFlatMapFunction[SensorReading, (Int, Long)] with ListCheckpointed[java.lang.Long] {
  //  子任务的索引号
  private lazy val subtaskIdx = getRuntimeContext.getIndexOfThisSubtask
  //  本地计数器变量
  private var highTempCounter = 0L

  override def flatMap(value: SensorReading, out: Collector[(Int, Long)]): Unit = {
    if (value.temperature > threshold) {
      //      如果超过阈值，计数器 +1
      highTempCounter += 1
      out.collect((subtaskIdx, highTempCounter))
    }
  }

  override def restoreState(state: java.util.List[java.lang.Long]): Unit = {
    highTempCounter = 0
    //    将状态恢复为列表中的全部long值之和
    for (cnt <- state.asScala) highTempCounter += cnt
  }

  override def snapshotState(chkpntId: Long, ts: Long): java.util.List[lang.Long] = {
    //    将一个包含单个数目值的列表作为状态快照
    java.util.Collections.singletonList(highTempCounter)
  }

}


