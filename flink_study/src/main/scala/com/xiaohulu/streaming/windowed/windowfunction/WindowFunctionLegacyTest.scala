package com.xiaohulu.streaming.windowed.windowfunction

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/30
  * \* Time: 16:02
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object WindowFunctionLegacyTest {
  def main(args: Array[String]): Unit = {

    /**
      * 在某些可以试用ProcessWindowFunction的地方，也可以使用WindowFunction。这个是较旧的版本，没有某些高级功能。
      * 注意：这里的keyBy中只能使用KeySelector指定key，不可以使用基于position。
      */
    //设置环境
    import org.apache.flink.api.scala._
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
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .apply(new UserDefineWindowFunction)


    stream.print()
    env.execute("word count")
  }
}

class UserDefineWindowFunction extends WindowFunction[(String, Int), String, String, TimeWindow] {
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[(String, Int)],
                     out: Collector[String]): Unit = {

    val sdf = new SimpleDateFormat("HH:mm:ss")
    val start = sdf.format(window.getStart)
    val end = sdf.format(window.getEnd)
    val maxTimestamp = sdf.format(window.maxTimestamp())
    println(s"key=$key,start:$start,end:$end,maxTimestamp:" + maxTimestamp)
    out.collect(s"$key,${input.map(_._2).sum}")
  }
}
