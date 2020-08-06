package com.xiaohulu.streaming.transformationed

import java.lang
import java.time.Duration

import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.{CoGroupFunction, JoinFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction

import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/8/3
  * \* Time: 18:27
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object StreamingJoinCoGroup {

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
    val stream_order = env.addSource(new SourceCoGroup_1)
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
          override def extractTimestamp(element: (String, Long), recordTimestamp: Long) = element._2
        }))
    val stream_user = env.addSource(new SourceCoGroup_2)
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
          override def extractTimestamp(element: (String, Long), recordTimestamp: Long) = element._2
        }))
    //coGroup：和join类似，不过CoGroupFunction和JoinFunction的参数不一样。coGroup是需要自己组装数据。
    //coGroup既会输出匹配的结果，也会输出未匹配的结果，给出的方式，一个迭代器，需要自己组装。这是和join的区别。
    val res_cogroup = stream_order.coGroup(stream_user)
      .where(order => order._2).equalTo(user => user._2)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2), Time.seconds(1)))
      .trigger(ProcessingTimeTrigger.create())
      .apply(new CoGroupFunction[(String, Long), (String, Long), (Long, String)] {
        override def coGroup(first: lang.Iterable[(String, Long)], second: lang.Iterable[(String, Long)], out: Collector[(Long, String)]) = {
          val arrayB: ArrayBuffer[String] = new ArrayBuffer[String]()

          first.asScala.foreach(tup => {
            arrayB += tup._1
          })
          second.asScala.foreach(tup => {
            arrayB += tup._1
          })
          out.collect(first.asScala.head._2, arrayB.mkString(","))
        }
      })

    res_cogroup.print()

    env.execute("StreamingDemoWithMyNoParallelSourceScala")


  }
}


class SourceCoGroup_1 extends SourceFunction[(String, Long)] {
  val array = Array(("python", 1596585950L), ("tensorflow", 1596585950L), ("pytorch", 1596585950L), ("pandas", 1596585950L), ("matplotlib", 1596585950L), ("sklearn", 1596585950L))

  override def run(ctx: SourceFunction.SourceContext[(String, Long)]): Unit = {
    while (true) {
      val index = scala.util.Random.nextInt(6)
      ctx.collect(array(index))
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {}
}

class SourceCoGroup_2 extends SourceFunction[(String, Long)] {
  val array = Array(("scala", 1596585950L), ("hadoop", 1596585950L), ("hbase", 1596585950L), ("kafka", 1596585950L), ("spark", 1596585950L), ("flink", 1596585950L), ("hive", 1596585950L))

  override def run(ctx: SourceFunction.SourceContext[(String, Long)]): Unit = {
    while (true) {
      val index = scala.util.Random.nextInt(6)
      ctx.collect(array(index))
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {}
}
