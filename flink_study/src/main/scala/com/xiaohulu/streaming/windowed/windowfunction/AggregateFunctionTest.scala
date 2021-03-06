package com.xiaohulu.streaming.windowed.windowfunction

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/28
  * \* Time: 15:43
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object AggregateFunctionTest {
  def main(args: Array[String]): Unit = {
    /**
      * AggregateFunction：更加通用，也更加复杂，通过WindowedStream的aggregate方法指定一个AggregateFunction来处理。
      * 其中实现AggregateFunction需要传入三个泛型，第一个表示源数据类型，第二个表示acc（accumulator）的类型，第三个是结果数据类型，
      * 并且要实现四个方法，
      * 也就是说add需要传入一条元素和当前累加的中间结果，且第一次add的acc是预先定义的createAccumulator，add输出的是中间状态的acc，
      * 一般来说，元素add完毕之后便会调用getResult计算自身业务想要的结果。简单实现一个AggregateFunction具备计算平均数如下：
      */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val stream = env.fromCollection(Seq(("qh1", 100L), ("qh1", 200L), ("qh1", 300L)))

    val result = stream.keyBy(_._1)
      .countWindow(3)
      .aggregate(new AverageAggregate)

    result.print() //2> 200.0

    env.execute("aggregate function demo")

  }
}


class AverageAggregate extends AggregateFunction[(String, Long), (Long, Long), Double] {
  //    add将每一个元素以某种方式添加到acc中，
  override def add(value: (String, Long), accumulator: (Long, Long)) = (accumulator._1 + value._2, accumulator._2 + 1L)

  //createAccumulator为初始化acc，其目的是用于add第一个元素，
  override def createAccumulator() = (0L, 0L)

  //getResult获取最终计算结果，
  override def getResult(accumulator: (Long, Long)) = accumulator._1.toDouble / accumulator._2

  // merge为合并acc；,将并行执行后的结果合并
  override def merge(a: (Long, Long), b: (Long, Long)) = (a._1 + b._1, a._2 + b._2)

}
