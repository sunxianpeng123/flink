package com.xiaohulu.streaming.windowed.windowfunction

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/29
  * \* Time: 16:41
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
object ProcessWinFuncWithAggFunc {
//TypeInformation.of()
  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {

    /**
      * AggregateFunction combined with ProcessWindowFunction
      * 该例通过定义AggregateFunction 求取平均数的逻辑，然后AggregateFunction 的输出会作为ProcessWindowFunction 的输入，ProcessWindowFunction 会将window触发时的平均值连同key一起作为输出。
      */
    //设置环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    //设置数据源
    val stream = env.addSource(new SourceFunction[String] {
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while (true) {
          ctx.collect("hello hadoop hello storm hello spark")
          Thread.sleep(1000)
        }
      }

      override def cancel(): Unit = {}
    })
      //计算逻辑
      .flatMap(_.split(" "))
      .map((_, scala.util.Random.nextInt(10)))
      .keyBy(_._1)
      //      stream.print()
      .timeWindow(Time.seconds(5))
      .aggregate(new MyAggAverageAggregateWithAggFunc, new MyProcessWinFunctionWithAggFunc)

    stream.print()
    env.execute("word count")
  }
}

class MyAggAverageAggregateWithAggFunc extends AggregateFunction[(String, Int), (Int, Int), Double] {
  override def add(in: (String, Int), acc: (Int, Int)) = (acc._1 + in._2, acc._2 + 1)

  override def createAccumulator() = (0, 0)

  override def getResult(acc: (Int, Int)) = acc._1.toDouble / acc._2

  override def merge(acc: (Int, Int), acc1: (Int, Int)) = (acc._1 + acc1._1, acc._2 + acc1._2)
}


class MyProcessWinFunctionWithAggFunc extends ProcessWindowFunction[Double, (String, Double), String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[Double], out: Collector[(String, Double)]): Unit = {
    val average = elements.iterator.next()
    out.collect(key, average)
  }
}
