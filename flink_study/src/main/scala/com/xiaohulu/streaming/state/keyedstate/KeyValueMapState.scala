package com.xiaohulu.streaming.state.keyedstate

import java.util.UUID

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
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
  * \* Date: 2020/5/20
  * \* Time: 15:09
  * \* To change this template use File | Settings | File Templates.
  * \* Description:也是实现同样的需求，不过这个其实会存在一个问题，因为mapState不同于上面的两个state，mapState的特点是相同的key它会做一个覆盖操作，这份数据，在Tuple2.of(1L, 3L)来之后，
  * Tuple2.of(1L, 5L)再过来，它就会把前面的3L替换成5L，而不是统计起来。其实这个就和Java的map一毛一样所以一句话解释就是，mapState中key相同的数据会处于同一个state，
  * 所以我们这次要采用字符串类型的key，设计成1_1,1_2,1_3这种形式
  * \*/
object KeyValueMapState {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val keyStream = env.fromElements((1L, 3L), (1L, 5L),
      (1L, 7L), (2L, 4L),
      (2L, 2L), (2L, 5L))
      .keyBy(0)
    //keyStream.print()

    val result = keyStream.flatMap(new CountWindowAverageWithMapState())
    result.print()
    //    输出结果
    //    6> (1,5.0)
    //    8> (2,3.6666666666666665)
    env.execute("ExampleManagedState")
  }
}

class CountWindowAverageWithMapState extends RichFlatMapFunction[(Long, Long), (Long, Double)] {
  // 用以保存每个 key 出现的次数，以及这个 key 对应的 value 的总值
  // managed keyed state
  //1. ValueState 保存的是对应的一个 key 的一个状态值
  private var mapState: MapState[String, Long] = _

  override def open(parameters: Configuration): Unit = {
    // 注册状态
    // 状态的名字 average
    // 状态存储的数据类型 createTypeInformation[(Long, Long)]
    val descriptor = new MapStateDescriptor("average", createTypeInformation[String], createTypeInformation[Long]);
    mapState = getRuntimeContext.getMapState(descriptor)
  }

  override def flatMap(input: (Long, Long), out: Collector[(Long, Double)]): Unit = {
    mapState.put(UUID.randomUUID().toString, input._2)
    val arrayList = Lists.newArrayList(mapState.values());

    // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
    if (arrayList.size() >= 3) {
      var count = 0L
      var sum = 0L
      arrayList.asScala.foreach(e => {
        count += 1L
        sum += e
      })
      val avg = sum.toDouble / count
      out.collect(input._1, avg)
      //清空状态值
      mapState.clear()
    }
  }
}