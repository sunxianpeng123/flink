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
object EventTimeWindow {
  def main(args: Array[String]): Unit = {
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        println("not port set ,give it default port 9000");
      }
        9000
    }
    //  获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    链接socket获取输入数据
    val stream = env.socketTextStream("masters", port, ',')
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(0)) {
          //watermark
          override def extractTimestamp(element: String): Long = {
            //输入数据格式 enenttime word --->例如：2019-06-07_18:33:15 hello
            val eventTime = element.split(" ")(0).toLong
            println(eventTime)
            eventTime

          }
        }
      ).map(item => (item.split(" ")(1), 1L)).keyBy(0)
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

