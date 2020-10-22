package com.xiaohulu.config

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/17
  * \* Time: 12:04
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object config {
  import org.apache.flink.api.scala._
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    /**
      * 设置并行度
      */

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    1、代码中
//    2 、提交flink任务的命令中
//    3、给算子单独设置并行度
    env.setParallelism(5)



  }
}

