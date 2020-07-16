package com.xiaohulu.streaming.state.keyedstate

import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.log4j.{Level, Logger}

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/5/21
  * \* Time: 10:18
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 此时我们想实现这么一个功能，比如我们的数据还是那份，我们要实现的效果是
  * (1,Contains:3 and 5 and 7)
  * (2,Contains:4 and 2 and 5)
  * \*/
object KeyValueReducingState {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val keyStream = env.fromElements((1L, 3L), (1L, 5L),
      (1L, 7L), (2L, 4L),
      (2L, 2L), (2L, 5L))
      .keyBy(0)
    //keyStream.print()

    val result = keyStream.flatMap(new SumWithReducingState())
    result.print()
    //    输出结果
//    8> (2,4)
//    6> (1,3)
//    8> (2,6)
//    6> (1,8)
//    8> (2,11)
//    6> (1,15)
    env.execute("ExampleManagedState")
  }
}


class SumWithReducingState extends  RichFlatMapFunction[(Long,Long),(Long,Long)]{
  var reducingState :ReducingState[Long] = _
  override def open(parameters: Configuration): Unit = {
  val  descriptor = new ReducingStateDescriptor("sum",//状态的名字
      new ReduceFunction[Long]() {//聚合函数
        override def reduce(value1: Long, value2: Long) = {
          value1 + value2
        }
      },createTypeInformation[Long])
    reducingState = getRuntimeContext.getReducingState(descriptor)
  }


  override def flatMap(value: (Long, Long), out: Collector[(Long, Long)]) = {
    //将数据放到状态中
    reducingState.add(value._2)
    out.collect((value._1, reducingState.get()))
  }
}

