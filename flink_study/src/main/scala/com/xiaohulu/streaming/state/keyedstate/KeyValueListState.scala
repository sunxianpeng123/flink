package com.xiaohulu.streaming.state.keyedstate

import java.util.Collections

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists
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
    val descriptor = new ListStateDescriptor[(Long, Long)]("average", createTypeInformation[(Long, Long)])
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






