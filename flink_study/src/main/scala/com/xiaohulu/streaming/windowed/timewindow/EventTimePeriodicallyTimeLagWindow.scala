package com.xiaohulu.streaming.windowed.timewindow

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
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
object EventTimeTimeLagWindow {
  def main(args: Array[String]): Unit = {
    /**
      * 该例子假设元素在有界延迟后到达，生成器生成的水印比处理时间滞后固定时间长度。
      * 比较容易理解，使用系统时间减去允许的延时时间作为 watermark 的时间。只跟当前系统时间有关系，如果大批事件出现延时的情况，可能很多在 watermark 的时间之后出现了，会被被丢弃。
      */
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => println("not port set ,give it default port 9000")
        9000
    }
    //    val stream = env.fromCollection(Seq(("qh1", 100L), ("qh1", 101L)))
    //  获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    链接socket获取输入数据
    //    val stream = env.socketTextStream("masters", port, ',')
    val stream = env.fromCollection(Seq(("qh1", 100L), ("qh1", 101L)))
      .assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator).keyBy(_._1)
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
class TimeLagWatermarkGenerator extends AssignerWithPeriodicWatermarks[(String, Long)] {

  val maxTimeLag = 5000L // 5 seconds

  override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
    element._2
  }

  override def getCurrentWatermark(): Watermark = {
    // return the watermark as current time minus the maximum time lag
    new Watermark(System.currentTimeMillis() - maxTimeLag)
  }
}

