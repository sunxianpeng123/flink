package com.xiaohulu.streaming.windowed.windowfunction

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
object ProcessWinFuncOnly {

  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {
    /**
      * ProcessWindowFunction:全量聚合函数
      * 通过实现 ProcessWindowFunction 完成基于窗口上的key的统计：包括求和，最小值，最大值，以及平均值等聚合指标，并获取窗口结束时间等元数据信息
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
      .timeWindow(Time.seconds(5), Time.seconds(5))
      .process(new MyProcessWinFunction)

    stream.print()
    env.execute("word count")
  }
}

//通过实现ProcessWindowFunction完成基于窗口上的key的统计：包括求和，最小值，最大值，以及平均值等聚合指标，并获取窗口结束时间等元数据信息
class MyProcessWinFunction extends ProcessWindowFunction[(String, Int), (String, Int, Int, Int, Long), String, TimeWindow] {
  //  Flink针对全量聚合计算提供了一个骨架抽象类ProcessWindowFunction，如果我们不需要操作状态数据，则只需要实现ProcessWindowFunction的process（）方法即可，在该方法中具体定义计算评估和输出的逻辑。
  override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int, Int, Int, Long)]): Unit = {
    var sum = 0
    var max = elements.head._2
    var min = elements.head._2
    elements.foreach(tup => {
      sum = sum + tup._2
      if (max < tup._2) max = tup._2
      if (min < tup._2) min = tup._2
    })
    // 求取窗口结束时间
    val winEndTime = context.window.getEnd
    out.collect((key, sum, max, min, winEndTime))
  }
}
