package com.xiaohulu.streaming.windowed.timewindow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/16
  * \* Time: 18:15
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object EventTimeSourceWatermark {
  def main(args: Array[String]): Unit = {
    //  获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)



  }
}

