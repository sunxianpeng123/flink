package com.xiaohulu.streaming.transformationed

import java.time.Duration

import com.xiaohulu.streaming.customsource.MyNoParallelSourceScala
import org.apache.flink.api.common.eventtime._
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
object StreamingJoinInternal {

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
    /**
      * KeyedStream,KeyedStream → DataStream
      * 在给定的时间边界内(默认包含边界)，相当于一个窗口，按照指定的key对两个KeyedStream进行join操作，把符合join条件的两个event拉到一起，然后怎么处理由用户你来定义。
      * key1 == key2 && e1.timestamp + lowerBound <= e2.timestamp <= e1.timestamp + upperBound
      * 场景：把一定时间范围内相关的分组数据拉成一个宽表
      */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置数据源
    val stream_order = env.addSource(new SourceInternalJoin_1)
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
          override def extractTimestamp(element: (String, Long), recordTimestamp: Long) = element._2
        }))
    val stream_user = env.addSource(new SourceInternalJoin_2)
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
          override def extractTimestamp(element: (String, Long), recordTimestamp: Long) = element._2
        }))

    //intervalJoin：是一个流join另一个流，不需要设置窗口，但是需要设置流join的时间范围（需要时间字段），仅支持Event Time的计算。
    //order 和 user 样例
    //    Order{orderId='1001', userId='1001', price='10', timestamp=1561024905185}
    //    user> (1001,caocao,20,1001,1001,10,1561023042640)

    //    between(Time.seconds(-60), Time.seconds(60))：相当于order.createTime - 60s < user.createTime < order.createTime + 60s
    //    lowerBoundExclusive()：取下边界，order.createTime - 60s <= user.createTime
    //    upperBoundExclusive()：取上边界， user.createTime <= order.createTime + 60s

    val res_intervaljoin = stream_order.keyBy(_._2)
      .intervalJoin(stream_user.keyBy(_._2))
      // between 只支持 event time
      .between(Time.seconds(-1), Time.seconds(1)) //between(Time.seconds(-60), Time.seconds(60))：相当于order.createTime - 60s < user.createTime < order.createTime + 60s
      .lowerBoundExclusive() //lowerBoundExclusive()：取下边界，order.createTime - 60s <= user.createTime
      .upperBoundExclusive() //upperBoundExclusive()：取上边界， user.createTime <= order.createTime + 60s
      .process(new ProcessJoinFunction[(String, Long), (String, Long), (Long, String, String)] {
      override def processElement(left: (String, Long), right: (String, Long), ctx: ProcessJoinFunction[(String, Long), (String, Long), (Long, String, String)]#Context, out: Collector[(Long, String, String)]) = {
        out.collect(left._2, left._1, right._1)
      }
    })

    res_intervaljoin.print()

    env.execute("StreamingDemoWithMyNoParallelSourceScala")


  }
}

class MyselfInternalJoinPeriodWatermark extends WatermarkStrategy[(String, Long)]() {
  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context) = {
    new WatermarkGenerator[(String, Long)] {
      var maxTimestamp: Long = _
      var delay = 3000L

      //onPeriodicEmit : 如果数据量比较大的时候，我们每条数据都生成一个水印的话，会影响性能，所以这里还有一个周期性生成水印的方法。这个水印的生成周期可以这样设置：env.getConfig().setAutoWatermarkInterval(5000L);
      override def onPeriodicEmit(watermarkOutput: WatermarkOutput) = watermarkOutput.emitWatermark(new Watermark(maxTimestamp - delay))

      //onEvent ：每个元素都会调用这个方法，如果我们想依赖每个元素生成一个水印，然后发射到下游(可选，就是看是否用output来收集水印)，我们可以实现这个方法.
      override def onEvent(t: (String, Long), l: Long, watermarkOutput: WatermarkOutput) = {
        maxTimestamp = Math.max(maxTimestamp, t._2)
      }
    }
  }
}


class SourceInternalJoin_1 extends SourceFunction[(String, Long)] {
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

class SourceInternalJoin_2 extends SourceFunction[(String, Long)] {
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
