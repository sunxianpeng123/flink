package com.xiaohulu.streaming.windowed.timewindow


import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/16
  * \* Time: 18:15
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * 1、我们需要手动实现一个时间戳提取器，它可以从事件中提取事件的产生时间。事件的格式为“a,事件产生时的时间戳”。
  * 时间戳提取器中的extractTimestamp方法获取时间戳。getCurrentWatermark方法可以暂时忽略不计。
  * 2、对输入流注册时间戳提取器
  * \*/
object EventTimeSourceWatermark {

  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {
    //  获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //    nc -l -p 9021
    // 最简单的流程处理，输入数据为“事件名称,事件产生的实际时间”
    // a,13 (在第13秒时输入)
    // a,16 (在第16秒时输入)
    // a,14 (在第19秒时输入)
    val input: DataStream[(String)] = env.socketTextStream("192.168.199.137", 9021)
    val stream = input.assignTimestampsAndWatermarks(new TimestampExtractor)
    val counts = stream.map(m => (m.split(",")(0), 1)).keyBy(0).timeWindow(Time.seconds(10), Time.seconds(5)).sum(1)
    counts.print()

    env.execute("TimeAndWindow")

  }
}

/**
  * 时间戳提取器，水印
  */
class TimestampExtractor extends AssignerWithPeriodicWatermarks[String] with Serializable {
  // 当 水印时间 大于等于 窗口的结束时间，开始触发窗口的计算。
  // 这里的水印使用的是系统时间，精确到毫秒
  // 所以事件时间的基准必须和水印时间一致，也是毫秒级时间戳
  override def getCurrentWatermark = {
    new Watermark(System.currentTimeMillis - 5000)
  }
  override def extractTimestamp(element: String, previousElementTimestamp: Long) = {
    // 自动获取当前时间整分钟的时间戳，yyyy-MM-dd HH:mm:00
    val baseTimeStringType = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date) + ":00"
    val baseTimeDateType = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(baseTimeStringType)

    // 计算事件的产生时间
    val baseTimestamp = baseTimeDateType.getTime
    val offsetMillis = 1000 * element.split(",")(1).toLong
    val newEventTimestamp = baseTimestamp.toLong + offsetMillis

    println(s"当前时间整分钟为$baseTimeStringType, 事件延迟的毫秒数为$offsetMillis," + s"事件产生时间为$newEventTimestamp，当前毫秒数为" + System.currentTimeMillis)

    newEventTimestamp
  }
}