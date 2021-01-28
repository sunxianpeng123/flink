package com.xiaohulu.streaming.transformed

import java.time.Duration

import com.xiaohulu.streaming.customsource.MyNoParallelSourceScala
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger
import org.apache.flink.util.Collector

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/8/3
  * \* Time: 18:27
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object StreamingJoin {

  import org.apache.flink.table.api._
  import org.apache.flink.table.api.bridge.scala._
  import org.apache.flink.streaming.api.scala._

  def main(args: Array[String]): Unit = {
    /**
      * Flink Stream join && intervalJoin && coGroup的区别
      * Flink DataStream Api提供了3中Stream join的算子，分别是join，intervalJoin和coGroup算子。
      * join：是一个流join另一个流，需要设置窗口，2个流join需要的key字段。使用的是innerJoin。对Processing Time和Event Time都支持。只输出条件匹配的元素对。
      * intervalJoin：是一个流join另一个流，不需要设置窗口，但是需要设置流join的时间范围（需要时间字段），仅支持Event Time的计算。
      * coGroup：和join类似，不过CoGroupFunction和JoinFunction的参数不一样。coGroup是需要自己组装数据。除了输出匹配的元素对以外，未能匹配的元素也会输出。
      * CoFlatMap：没有匹配条件，不进行匹配，分别处理两个流的元素。在此基础上完全可以实现join和cogroup的功能，比他们使用上更加自由。
      */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置数据源
    val stream_order = env.addSource(new SourceJoin_1) //.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[String](Duration.ofSeconds(3)))
    val stream_user = env.addSource(new SourceJoin_2) //.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[String](Duration.ofSeconds(3)))


    //    stream1.print()
    println("======================Join, inner join==========================")
    //join：是一个流join另一个流，需要设置窗口，2个流join需要的key字段。使用的是innerJoin。对Processing Time和Event Time都支持。
    val res_join = stream_order.join(stream_user)
      .where(x1 => x1._2).equalTo(y1 => y1._2) //join的条件stream1中的某个字段和stream2中的字段值相等
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2), Time.seconds(1))) // 指定window，stream1和stream2中的数据会进入到该window中。只有该window中的数据才会被后续操作join
      .trigger(ProcessingTimeTrigger.create())
      //    可以 用模式匹配
      //    .apply((x, y) => (x._2, x._1, y._1))// 捕获到匹配的数据t1和t2，在这里可以进行组装等操作
      //    可以通过实现JoinFunction
      .apply(new JoinFunction[(String, Int), (String, Int), (Int, String, String)] {
      override def join(first: (String, Int), second: (String, Int)) = (first._2, first._1, second._1) // 捕获到匹配的数据t1和t2，在这里可以进行组装等操作
    })
    res_join.print()

    env.execute("StreamingDemoWithMyNoParallelSourceScala")
  }
}

class SourceJoin_1 extends SourceFunction[(String, Int)] {
  val array = Array(("python", 1), ("tensorflow", 2), ("pytorch", 3), ("pandas", 4), ("matplotlib", 5), ("sklearn", 6))

  override def run(ctx: SourceFunction.SourceContext[(String, Int)]): Unit = {
    while (true) {
      val index = scala.util.Random.nextInt(6)
      ctx.collect(array(index))
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {}
}

class SourceJoin_2 extends SourceFunction[(String, Int)] {
  val array = Array(("scala", 1), ("hadoop", 2), ("hbase", 3), ("kafka", 4), ("spark", 5), ("flink", 6), ("hive", 6))

  override def run(ctx: SourceFunction.SourceContext[(String, Int)]): Unit = {
    while (true) {
      val index = scala.util.Random.nextInt(6)
      ctx.collect(array(index))
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {}
}
