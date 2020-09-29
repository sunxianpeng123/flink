package com.xiaohulu.streaming.state.keyedstate

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
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
  * \* Description: AggregatingState
  * 此时我们想实现这么一个功能，比如我们的数据还是那份，我们要实现的效果是
  * (1,Contains:3 and 5 and 7)
  * (2,Contains:4 and 2 and 5)
  * \*/
object KeyValueAggregatingState {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val keyStream = env.fromElements((1L, 3L), (1L, 5L),
      (1L, 7L), (2L, 4L),
      (2L, 2L), (2L, 5L))
      .keyBy(0)
    //keyStream.print()

    val result = keyStream.flatMap(new LinkWithAggregatingState())
    result.print()
    //    输出结果
    //    6> (1,Contains:3)
    //    6> (1,Contains:3 and 5)
    //    8> (2,Contains:4)
    //    8> (2,Contains:4 and 2)
    //    6> (1,Contains:3 and 5 and 7)
    //    8> (2,Contains:4 and 2 and 5)
    env.execute("ExampleManagedState")
  }
}

//https://zhuanlan.zhihu.com/p/134661257
class LinkWithAggregatingState extends RichFlatMapFunction[(Long, Long), (Long, String)] {
  var aggregatingState: AggregatingState[Long, String] = _

  override def open(parameters: Configuration): Unit = {
    val stateTtlConfig = StateTtlConfig
      //指定ttl时间为10秒
      .newBuilder(Time.seconds(10))
      //指定ttl刷新时只对创建和写入操作有效,设置状态的声明周期,在规定时间内及时的清理状态数据
      //      OnCreateAndWrite 仅在创建和写入时更新ttl
      //      OnReadAndWrite 所有读与写操作都更新ttl
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      //指定状态可见性为永远不返回过期数据
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build()

    //new AggregatingStateDescriptor[(Long, String, String)]
    // 第一个是输入数据类型，第三个是输出数据类型，中间是累加的一个辅助变量，此时你要实现一个new AggregateFunction()这样的接口，你会发现一下子搞出4个需要实现的方法

    val descriptor = new AggregatingStateDescriptor[Long, String, String]("totalStr", new AggregateFunction[Long, String, String] {
      override def add(value: Long, accumulator: String) = if ("Contains:".equals(accumulator)) accumulator + value else accumulator + " and " + value

      override def createAccumulator() = "Contains:"

      override def getResult(accumulator: String) = accumulator

      override def merge(a: String, b: String) = {
        if ("Contains:".equals(a)) b
        if ("Contains:".equals(b)) a
        val fields = a.split(":")
        b + fields(1)
      }
    }, classOf[String])
    descriptor.enableTimeToLive(stateTtlConfig)
    aggregatingState = getRuntimeContext.getAggregatingState(descriptor);
  }

  override def flatMap(value: (Long, Long), out: Collector[(Long, String)]) = {
    aggregatingState.add(value._2)
    out.collect((value._1, aggregatingState.get()))
  }
}


