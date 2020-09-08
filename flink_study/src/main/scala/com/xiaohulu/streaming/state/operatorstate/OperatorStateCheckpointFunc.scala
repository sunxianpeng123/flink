package com.xiaohulu.streaming.state.operatorstate

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import scala.collection.JavaConverters._

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/9/2
  * \* Time: 17:31
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object OperatorStateCheckpointFunc {
  def main(args: Array[String]): Unit = {


  }
}

/**
  * 利用operator State 统计输入到算子的数据量
  *
  * @param numElements
  */
private class CheckpointCount(val numElements: Int) extends FlatMapFunction[(Int, Long), (Int, Long, Long)] with CheckpointedFunction {
  //  定义算子实例本地变量，存储operator 数据数量
  private var operatorCount = 0L
  //  存储和key相关的值
  private var keyedState: ValueState[Long] = _
  //  存储算子的状态值
  private var operatorState: ListState[Long] = _

  override def flatMap(value: (Int, Long), out: Collector[(Int, Long, Long)]) = {
    //
    val keyedCount = keyedState.value() + 1
    //    更新keyedState数量
    keyedState.update(keyedCount)
    //    更新本地算子operatorCount值
    operatorCount = operatorCount + 1
    //    输出结果,包括id,id对应的数量统计keyedCount,算子输入数据的数量统计operatorCount
    out.collect(value._1, keyedCount, operatorCount)
  }

  //      当发送snapshot时，将operatorCount添加到operatorState中
  override def snapshotState(context: FunctionSnapshotContext) = {
    //    清理掉上一次存储在checkpoint中的operatorState数据
    operatorState.clear()
    //    添加并更新本次算子中需要checkpoint的operatorCount
    operatorState.add(operatorCount)
  }

  //初始化状态数据，系统重启会调用该方法，恢复keyedState和operatorState
  override def initializeState(context: FunctionInitializationContext) = {
    //    定义并获取keyedState
    keyedState = context.getKeyedStateStore.getState(new ValueStateDescriptor[Long]("keyedState", createTypeInformation[Long]))
    //    定义并获取operatorState
    operatorState = context.getOperatorStateStore.getListState(new ListStateDescriptor[Long]("operatorState", createTypeInformation[Long]))
    //    定义在Restored过程中，从operatorState中恢复数据的逻辑
    if (context.isRestored) {
      operatorCount = operatorState.get().asScala.sum
    }
  }
}
