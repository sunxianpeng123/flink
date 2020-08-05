package com.xiaohulu.streaming.state.keyedstate

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.log4j.{Level, Logger}

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/5/19
  * \* Time: 14:08
  * \* To change this template use File | Settings | File Templates.
  * \* Description:当接收到的相同 key 的元素个数等于 3 个或者超过 3 个的时候，就计算这些元素的 value 的平均值。计算 keyed stream 中每 3 个元素的 value 的平均值
  * \*/
object KeyValueValueState {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val keyStream = env.fromElements((1L, 3L), (1L, 5L),
      (1L, 7L), (2L, 4L),
      (2L, 2L), (2L, 5L))
      .keyBy(_._1)
    //      .keyBy(0)
    //keyStream.print()

    val result = keyStream.flatMap(new CountWindowAverageWithValueState())
    result.print()
    //    输出结果
    //    6> (1,5.0)
    //    8> (2,3.6666666666666665)
    env.execute("ExampleManagedState")
  }
}

class CountWindowAverageWithValueState extends RichFlatMapFunction[(Long, Long), (Long, Double)] {
  // 用以保存每个 key 出现的次数，以及这个 key 对应的 value 的总值
  // managed keyed state
  //1. ValueState 保存的是对应的一个 key 的一个状态值
  private var countAndSum: ValueState[(Long, Long)] = _

  override def open(parameters: Configuration): Unit = {
    // 注册状态
    // 状态的名字 average
    // 状态存储的数据类型 createTypeInformation[(Long, Long)]
    val descriptor = new ValueStateDescriptor[(Long, Long)]("average", createTypeInformation[(Long, Long)])
    countAndSum = getRuntimeContext.getState(descriptor)
  }

  override def flatMap(input: (Long, Long), out: Collector[(Long, Double)]): Unit = {
    // 拿到当前的 key 的状态值
    var currentState = countAndSum.value
    // 如果状态值还没有初始化，则初始化
    if (currentState == null) currentState = (0L, 0L)
    // 更新状态值中的元素的个数, 更新状态值中的总值
    val count = currentState._1 + 1
    val sum = currentState._2 + input._2
    currentState = (count, sum)
    // 更新状态
    countAndSum.update(currentState)
    // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
    if (currentState._1 >= 3) {
      //      val keyStream = env.fromElements((1L, 3L), (1L, 5L),
      //      (1L, 7L), (2L, 4L),
      //      (2L, 2L), (2L, 5L))
      //此处除法会保留整数
      val avg = currentState._2.toDouble / currentState._1
      // 输出 key 及其对应的平均值
      out.collect((input._1, avg))
      //清空状态值
      countAndSum.clear()
    }
  }
}
