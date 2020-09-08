//package com.xiaohulu.streaming.state.operatorstate
//
//import java.util
//import java.util.Collections
//
//import org.apache.flink.api.common.functions.FlatMapFunction
//import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
//import org.apache.flink.api.scala.createTypeInformation
//import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
//import org.apache.flink.streaming.api.checkpoint.{CheckpointedFunction, ListCheckpointed}
//import org.apache.flink.util.Collector
//import org.apache.flink.api.scala._
//
//import scala.collection.JavaConverters._
//
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: sunxianpeng
//  * \* Date: 2020/9/2
//  * \* Time: 18:55
//  * \* To change this template use File | Settings | File Templates.
//  * \* Description:
//  * \*/
//object OperatorStateListCheckState {
//  def main(args: Array[String]): Unit = {
//
//
//  }
//}
//
///**
//  * 利用operator State 统计输入到算子的数据量
//  *
//  * @param
//  */
///**
//  *
//  */
//class NumberRecordsCount extends FlatMapFunction[(String, Long), (String, Long)] with ListCheckpointed[Long] {
//
//  private var numberRecordsCount = 0L
//
//  override def flatMap(value: (String, Long), out: Collector[(String, Long)]) = {
//    //  接入上一条记录进行统计，并输出
//    numberRecordsCount += 1
//    out.collect(value._1, numberRecordsCount)
//  }
//
//  override def restoreState(state: util.List[Long]) = {
//    numberRecordsCount = 0L
//    val iterator = state.iterator()
//    while (iterator.hasNext) {
//      //  从状态中恢复numberRecordCount数据
//      val count = iterator.next()
//      numberRecordsCount += count
//    }
//  }
//
//  override def snapshotState(checkpointId: Long, timestamp: Long): java.util.List[Long] = {
//    //    snapshot 状态的过程中将 numberRecordCount 写入
//    Collections.singletonList(numberRecordsCount)
//  }
//}
//
