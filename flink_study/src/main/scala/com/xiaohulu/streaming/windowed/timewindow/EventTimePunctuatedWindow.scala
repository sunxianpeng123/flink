package com.xiaohulu.streaming.windowed.timewindow

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
;

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/6/7
  * \* Time: 18:16
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
object EventTimeBoundedWindow {
  def main(args: Array[String]): Unit = {
    /**
      * 每个事件都可以生成水印。但是，由于水印会导致一些后续的计算，因此过多的水印会降低性能。
      */
    //  获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    链接socket获取输入数据
    //    val stream = env.socketTextStream("masters", port, ',')
    val stream = env.fromCollection(Seq(("qh1", 100L), ("qh1", 101L)))
      .assignTimestampsAndWatermarks(new PunctuatedAssigner).keyBy(_._1)
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

class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[(String, Long)] {

  override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
    element._2
  }
  override def checkAndGetNextWatermark(lastElement: (String, Long), extractedTimestamp: Long): Watermark = {
    var res: Watermark = null
    if (lastElement != null) res = new Watermark(extractedTimestamp)
    res
  }
}

