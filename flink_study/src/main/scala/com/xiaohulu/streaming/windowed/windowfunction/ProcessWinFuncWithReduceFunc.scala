package com.xiaohulu.streaming.windowed.windowfunction

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
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
object ProcessWinFuncWithReduceFunc {

  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {

    /**
      * ReduceFunction combined with ProcessWindowFunction
      * 该例通过定义ReduceFunction 求取最大值
      * 定义ProcessWindowFunction从窗口元数据中获取窗口结束时间，然后将结束时间和ReduceFunction 的最大值结果组合成一个新的Tuple返回。
      * 同样的，ReduceFunction 的输出会作为ProcessWindowFunction的输入，
      * 同理FoldFunction也可以按照同样的方式和ProcessWindowFunction 整合，在实现增量聚合计算的同时，也可以操作窗口中的元数据信息以及状态数据。
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
      .reduce(new MyReduceWithReduceFunc, new MyProcessWinFunctionWithReduceFunc)

    stream.print()
    env.execute("word count")
  }
}

class MyReduceWithReduceFunc extends ReduceFunction[(String, Int)] {
  override def reduce(t: (String, Int), t1: (String, Int)) = if (t._2 > t1._2) t else t1
}

class MyProcessWinFunctionWithReduceFunc extends ProcessWindowFunction[(String, Int), (Long, (String, Int)), String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(Long, (String, Int))]): Unit = {
    val max = elements.head
    out.collect((context.window.getEnd, max))
  }
}
