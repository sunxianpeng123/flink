//package com.xiaohulu.streaming.windowed.timewindow
//
//import org.apache.flink.streaming.api.functions.source.SourceFunction
//import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
//import org.apache.flink.streaming.api.watermark.Watermark
//
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: sunxianpeng
//  * \* Date: 2020/7/17
//  * \* Time: 11:02
//  * \* To change this template use File | Settings | File Templates.
//  * \* Description:
//  * \*/
//class EventTimeSourceWatermarkSource extends SourceFunction[(String, Long)] {
//  val str = "abcdefghijklmnopqrstuvwxyz"
//  var res: (String, Long) = _
//  var isRunning = true
//
//  override def run(ctx: SourceContext[(String, Long)]) = {
//    while (isRunning) {
//      val rand = scala.util.Random.nextInt(26)
//      val num = rand % 26
//      val c = str.charAt(num).toString
//      res = (c, num)
//      val eventTime = System.currentTimeMillis()
//      val watermark = 10
//      //      ctx.collect(res)
//      ctx.collectWithTimestamp(res, eventTime)
//      ctx.emitWatermark(new Watermark(watermark))
//      Thread.sleep(1000)
//
//    }
//
//  }
//  override def cancel() = {
//    isRunning = false
//  }
//}
