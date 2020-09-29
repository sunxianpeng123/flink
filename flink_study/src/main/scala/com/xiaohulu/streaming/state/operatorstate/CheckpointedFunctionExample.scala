package com.xiaohulu.streaming.state.operatorstate

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._

/**
  * 运行时，动态配置传感器阈值
  */
object CheckpointedFunctionExample {

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
      .flatMap(new HighTempCounter(10.0))

    highTempCnts.print()
    env.execute()
  }
}

class HighTempCounter(val threshold: Double)
  extends FlatMapFunction[SensorReading, (String, Long, Long)]
    with CheckpointedFunction {

  // local variable for the operator high temperature cnt  //  定义算子实例本地变量，存储operator 数据数量
  var opHighTempCnt: Long = 0
  //  存储和key相关的值
  var keyedCntState: ValueState[Long] = _
  //  存储算子的状态值
  var opCntState: ListState[Long] = _

  override def flatMap(v: SensorReading, out: Collector[(String, Long, Long)]): Unit = {
    if (v.temperature > threshold) {
      // update local operator high temp counter    //    更新本地算子operatorCount值
      opHighTempCnt += 1
      // update keyed high temp counter    //    更新keyedState数量
      val keyHighTempCnt = keyedCntState.value() + 1
      keyedCntState.update(keyHighTempCnt)

      // emit new counters    //    输出结果,包括id,id对应的数量统计keyedCount,算子输入数据的数量统计operatorCount
      out.collect((v.id, keyHighTempCnt, opHighTempCnt))
    }
  }

  //当初始化自定义函数时会调用，初始化状态数据，系统重启会调用该方法，恢复keyedState和operatorState
  override def initializeState(initContext: FunctionInitializationContext): Unit = {
    // initialize keyed state,初始化键值状态    //    定义并获取keyedState
    val keyCntDescriptor = new ValueStateDescriptor[Long]("keyedCnt", classOf[Long])
    keyedCntState = initContext.getKeyedStateStore.getState(keyCntDescriptor)

    // initialize operator state，初始化operator状态    //    定义并获取operatorState
    val opCntDescriptor = new ListStateDescriptor[Long]("opCnt", classOf[Long])
    opCntState = initContext.getOperatorStateStore.getListState(opCntDescriptor)
    // initialize local variable with state，初始化本地变量状态    //    定义在Restored过程中，从operatorState中恢复数据的逻辑
    opHighTempCnt = opCntState.get().asScala.sum
  }

  //checkpoint触发时调用，当发送snapshot时，将operatorCount添加到operatorState中
  override def snapshotState(snapshotContext: FunctionSnapshotContext): Unit = {
    // update operator state with local state    //    清理掉上一次存储在checkpoint中的operatorState数据
    opCntState.clear()
    //    添加并更新本次算子中需要checkpoint的operatorCount
    opCntState.add(opHighTempCnt)
  }
}

