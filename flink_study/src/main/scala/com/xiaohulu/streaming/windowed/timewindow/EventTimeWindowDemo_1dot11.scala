package com.xiaohulu.streaming.windowed.timewindow

import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/6/7
  * \* Time: 18:16
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object EventTimeWindowDemo_1dot11 {
  def main(args: Array[String]): Unit = {
    /**
      * 在这个onEvent方法里，我们从每个元素里抽取了一个时间字段，但是我们并没有生成水印发射给下游，
      * 而是自己保存了在一个变量里，在onPeriodicEmit方法里，使用最大的日志时间减去我们想要的延迟时间作为水印发射给下游。
      */
    //  获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    链接socket获取输入数据
    //    val stream = env.socketTextStream("masters", port, ',')
    val stream = env.fromCollection(Seq(("qh1", 100L), ("qh1", 101L)))
      .assignTimestampsAndWatermarks(new MyselfPeriodWatermarkDemo).keyBy(_._1)

    //    //指定滚动窗口
    //    val streamWindow = stream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
    //    val streamWindow2 = stream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
    //指定滑动窗口
    //    val streamWindow = stream.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    //指定会话窗口,相邻两条超过指定时间会执行
    val streamWindow = stream.window(EventTimeSessionWindows.withGap(Time.seconds(5)))
    val streamReduce = streamWindow.reduce((item1, item2) => (item1._1, item1._2 + item2._2))

    streamReduce.print()

    env.execute("FlinkSource01")
  }
}

//class MyselfPeriodWatermarkDemo extends WatermarkStrategy[(String, Long)]() {
//  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context) = {
//    new WatermarkGenerator[(String, Long)] {
//      var maxTimestamp: Long = _
//      var delay = 3000L
//
//      //onPeriodicEmit : 如果数据量比较大的时候，我们每条数据都生成一个水印的话，会影响性能，所以这里还有一个周期性生成水印的方法。这个水印的生成周期可以这样设置：env.getConfig().setAutoWatermarkInterval(5000L);
//      override def onPeriodicEmit(watermarkOutput: WatermarkOutput) = watermarkOutput.emitWatermark(new Watermark(maxTimestamp - delay))
//
//      //onEvent ：每个元素都会调用这个方法，如果我们想依赖每个元素生成一个水印，然后发射到下游(可选，就是看是否用output来收集水印)，我们可以实现这个方法.
//      override def onEvent(t: (String, Long), l: Long, watermarkOutput: WatermarkOutput) = {
//        maxTimestamp = Math.max(maxTimestamp, t._2)
//      }
//    }
//  }
//}

