//package com.xiaohulu.streaming.state.operatorstate
//
//import java.{lang, util}
//
//import org.apache.flink.api.common.functions.RichFlatMapFunction
//import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
//import org.apache.flink.util.Collector
//import scala.collection.JavaConverters._
//
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: sunxianpeng
//  * \* Date: 2020/7/2
//  * \* Time: 18:58
//  * \* To change this template use File | Settings | File Templates.
//  * \* Description:
//  * \*/
//object ListCheckpointedTest {
//  def main(args: Array[String]): Unit = {
//
//  }
//}
//
//class HighTempCounter(val threshold: Double) extends RichFlatMapFunction[SensorReadingListCheckpointed, (Int, Long)] with ListCheckpointed[java.lang.Long] {
//  //  子任务的索引号
//  private lazy val subtaskIdx = getRuntimeContext.getIndexOfThisSubtask
//  //  本地计数器变量
//  private var highTempCounter = 0L
//
//  override def flatMap(value: SensorReadingListCheckpointed, out: Collector[(Int, Long)]): Unit = {
//    if (value.temperature > threshold) {
//      //      如果超过阈值，计数器 +1
//      highTempCounter += 1
//      out.collect((subtaskIdx, highTempCounter))
//    }
//  }
//
//  override def restoreState(state: util.List[java.lang.Long]): Unit = {
//    highTempCounter = 0
//    //    将状态恢复为列表中的全部long值之和
//    for (cnt <- state.asScala) highTempCounter += cnt
//  }
//
//  override def snapshotState(chkpntId: Long, ts: Long): util.List[lang.Long] = {
//    //    将一个包含单个数目值的列表作为状态快照
//    java.util.Collections.singletonList(highTempCounter)
//  }
//
//}
//
//class SensorReadingListCheckpointed {
//  var temperature: Double = _
//}
