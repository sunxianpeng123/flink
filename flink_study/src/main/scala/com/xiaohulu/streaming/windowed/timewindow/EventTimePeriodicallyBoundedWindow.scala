package com.xiaohulu.streaming.windowed.timewindow

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
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
object EventTimePeriodicallyBoundedWindow {
  def main(args: Array[String]): Unit = {
    /**
      * 该例子假定元素到达时在一定程度上是无序的，某个时间戳t的最后达到元素相比时间戳t的最早到达元素，最大延迟n毫秒。
      * 第一个例子，在当前事件的事件时间和当前最大时间（记录最大的事件时间）中取最大值，得到最大的事件时间。
      * 用这个最大值减去一个允许的延时时间作为 watermark 时间。同样的如果大批事件发生延时，那么对应的 watermark 的时间就会向后推。
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
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.milliseconds(0)) {
          //watermark
          override def extractTimestamp(element: (String, Long)): Long = {
            //输入数据格式 enenttime word --->例如：2019-06-07_18:33:15 hello
            val eventTime = element._2
            println(eventTime)
            eventTime
          }
        }
      ).keyBy(_._1)
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

