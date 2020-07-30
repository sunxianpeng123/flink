package com.xiaohulu.streaming.windowed.windowfunction

import java.lang

import org.apache.flink.api.common.functions.AggregateFunction

//import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/29
  * \* Time: 15:33
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * 1、在某些业务场景下，统计更复杂的指标，就可能会依赖窗口中所有的数据元素，以及可能会需要操作窗口中的状态数据和窗口元数据，
  * 全量聚合函数ProcessWindowFunction能够提供类似这种支持。ProcessWindowFunction的简单应用如：统计窗口数据元素中某一字段的中位数和众数。
  * 2、Flink针对全量聚合计算提供了一个骨架抽象类ProcessWindowFunction，如果我们不需要操作状态数据，
  * 则只需要实现ProcessWindowFunction的process（）方法即可，在该方法中具体定义计算评估和输出的逻辑。
  * \*/
object ProcessWindowFunctionTest {
  def main(args: Array[String]): Unit = {

    /**
      * ProcessWindowFunction:全量聚合函数
      * 1、通过实现 ProcessWindowFunction 完成基于窗口上的key的统计：包括求和，最小值，最大值，以及平均值等聚合指标，并获取窗口结束时间等元数据信息
      * 2、ProcessWindowFunction with Incremental Aggregation
      *     增量聚合函数由于是基于中间状态计算，因此性能较好，但是灵活性却不及ProcessWindowFunction；缺失了对窗口状态数据的操作以及对窗口中元数据信息的获取等。
      *     但是使用全量聚合函数去完成一些基础的增量统计运算又相对比较浪费资源，性能低于增量。
      *     因此 Flink 提供了一种方式，可以将 Incremental Aggregation Function 和 ProcessWindowFunction 整合起来，充分利用这两种计算方式的优势去处理数据。
      * 3、AggregateFunction combined with ProcessWindowFunction
      *     该例通过定义 AggregateFunction 求取平均数的逻辑，然后 AggregateFunction 的输出会作为 ProcessWindowFunction 的输入，ProcessWindowFunction 会将window触发时的平均值连同key一起作为输出。
      */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val stream = env.fromCollection(Seq(("qh1", 100L), ("qh1", 200L), ("qh1", 300L), ("qh1", 400L), ("qh1", 500L)))

//    val result = stream.keyBy(t => t._1).timeWindow(Time.seconds(2)).aggregate(new MyAverageAggregate, new MyProcessWindowFunction)


  }
}

class MyAverageAggregate extends AggregateFunction[(String, Long), (Long, Long), Double] {
  override def add(value: (String, Long), accumulator: (Long, Long)) = (accumulator._1 + value._2, accumulator._2 + 1)

  override def createAccumulator() = (0L, 0L)

  override def getResult(accumulator: (Long, Long)) = accumulator._1.toDouble / accumulator._2

  override def merge(a: (Long, Long), b: (Long, Long)) = {
    (a._1 + b._1, a._2 + b._2)
  }
}
class MyProcessWindowFunction extends ProcessWindowFunction[Double,(String,Double),String,TimeWindow]{
//  Flink针对全量聚合计算提供了一个骨架抽象类ProcessWindowFunction，如果我们不需要操作状态数据，则只需要实现ProcessWindowFunction的process（）方法即可，在该方法中具体定义计算评估和输出的逻辑。
  override def process(key: String, context: ProcessWindowFunction[Double, (String, Double), String, TimeWindow]#Context, elements: lang.Iterable[Double], out: Collector[(String, Double)]) = {
    val average = elements.iterator().next()

    out.collect((key,average))
  }
}
