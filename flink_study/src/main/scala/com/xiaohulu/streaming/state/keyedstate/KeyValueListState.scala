package com.xiaohulu.streaming.state.keyedstate

import java.util.Collections

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.collect.Lists
//import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConverters._

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/5/14
  * \* Time: 17:51
  * \* To change this template use File | Settings | File Templates.
  * \* Description:当接收到的相同 key 的元素个数等于 3 个或者超过 3 个的时候，就计算这些元素的 value 的平均值。计算 keyed stream 中每 3 个元素的 value 的平均值
  */
object KeyValueListState {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val keyStream = env.fromElements((1L, 3L), (1L, 5L),
      (1L, 7L), (2L, 4L),
      (2L, 2L), (2L, 5L))
      .keyBy(_._1)
    //keyStream.print()

    val result = keyStream.flatMap(new CountWindowAverageWithListState())
    result.print()
    //    输出结果
    //    6> (1,5.0)
    //    8> (2,3.6666666666666665)

    env.execute("ExampleManagedState")

  }
}

class CountWindowAverageWithListState extends RichFlatMapFunction[(Long, Long), (Long, Double)] {
  //List里面保存这所有的key出现的次数
  var elementByKey: ListState[(Long, Long)] = _

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

    val descriptor = new ListStateDescriptor[(Long, Long)]("average", createTypeInformation[(Long, Long)])
    descriptor.enableTimeToLive(stateTtlConfig)
    elementByKey = getRuntimeContext.getListState(descriptor)
  }

  override def flatMap(in: (Long, Long), collector: Collector[(Long, Double)]): Unit = {
    val currentState = elementByKey.get()
    //初始化
    if (currentState == null) elementByKey.addAll(Collections.emptyList())
    //更新状态
    elementByKey.add(in)
    val allElement = Lists.newArrayList(elementByKey.get())
    if (allElement.size() >= 3) {
      var count = 0L
      var sum = 0L
      allElement.asScala.foreach(e => {
        count += 1L
        sum += e._2
      })
      val avg = sum.toDouble / count
      collector.collect(in._1, avg)
      //清除数据
      elementByKey.clear()
    }
  }
}






