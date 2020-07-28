package com.xiaohulu.extractor

import java.text.SimpleDateFormat
import java.util.Date

import com.xiaohulu.bean.AnchorResultBean
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/27
  * \* Time: 11:45
  * \* To change this template use File | Settings | File Templates.
  * \* Description:  时间戳提取器，水印
  * \*/
class DyAnchorExtractor extends AssignerWithPeriodicWatermarks[AnchorResultBean] with Serializable{
    // 当 水印时间 大于等于 窗口的结束时间，开始触发窗口的计算。
    // 这里的水印使用的是系统时间，精确到毫秒
    // 所以事件时间的基准必须和水印时间一致，也是毫秒级时间戳
    override def getCurrentWatermark = {
      new Watermark(System.currentTimeMillis - 5000)
    }
    override def extractTimestamp(element: AnchorResultBean, previousElementTimestamp: Long) = {
      // 自动获取当前时间整分钟的时间戳，yyyy-MM-dd HH:mm:00
      val baseTimeStringType = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date) + ":00"
      val baseTimeDateType = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(baseTimeStringType)

      // 计算事件的产生时间
      val baseTimestamp = baseTimeDateType.getTime
      val offsetMillis = 1000 * element.timestamp.toLong
      val newEventTimestamp = baseTimestamp.toLong + offsetMillis

//      println(s"当前时间整分钟为$baseTimeStringType, 事件延迟的毫秒数为$offsetMillis," + s"事件产生时间为$newEventTimestamp，当前毫秒数为" + System.currentTimeMillis)

      newEventTimestamp

  }
}

