package com.xiaohulu.streaming.windowed.windowfunction

import java.lang

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
object ProcessWinFuncDemo {

  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {
    //
    //    /**
    //      * ProcessWindowFunction:全量聚合函数
    //      * 通过实现 ProcessWindowFunction 完成基于窗口上的key的统计：包括求和，最小值，最大值，以及平均值等聚合指标，并获取窗口结束时间等元数据信息
    //      */
    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    import org.apache.flink.api.scala._
    //    val stream = env.fromCollection(Seq(("qh1", 100L), ("qh1", 200L), ("qh1", 300L), ("qh1", 400L), ("qh1", 500L)))
    //    val func = new MyProcessWinFunction
    //    val result = stream.keyBy(_._1).timeWindow(Time.seconds(10))
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
      .map((_, 1))
      .keyBy(_._1)
      //      stream.print()
      .timeWindow(Time.seconds(5), Time.seconds(5))
      .process(
        new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
            var value = 0
            elements.foreach(tup => {
              value = value + tup._2
            })
            out.collect(key, value)
          }
        }
      )


    stream.print()
    env.execute("word count")
  }
}

////通过实现ProcessWindowFunction完成基于窗口上的key的统计：包括求和，最小值，最大值，以及平均值等聚合指标，并获取窗口结束时间等元数据信息
//class MyProcessWinFunction extends ProcessWindowFunction[(String, Long), (String, Long, Long, Long, Long), String, TimeWindow] {
//  //  Flink针对全量聚合计算提供了一个骨架抽象类ProcessWindowFunction，如果我们不需要操作状态数据，则只需要实现ProcessWindowFunction的process（）方法即可，在该方法中具体定义计算评估和输出的逻辑。
//  override def process(key: String,
//                       context: ProcessWindowFunction[(String, Long), (String, Long, Long, Long, Long), String, TimeWindow]#Context,
//                       elements: lang.Iterable[(String, Long)],
//                       out: Collector[(String, Long, Long, Long, Long)]) = {
//    var sum = 0L
//    var max = elements.iterator().next()._2
//    var min = elements.iterator().next()._2
//    val iter = elements.iterator()
//    while (iter.hasNext) {
//      val tup = iter.next()
//      sum = sum + tup._2
//      if (max < tup._2) max = tup._2
//      if (min > tup._2) min = tup._2
//    }
//    // 求取窗口结束时间
//    val winEndTime = context.window.getEnd
//    // 返回计算结果
//    out.collect((key, sum, max, min, winEndTime))
//  }
//}
