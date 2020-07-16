package com.xiaohulu.tableapi

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/10/19
  * \* Time: 22:39
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object JoinTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //获取接口传送的数据
    val dataStream1 = env.readTextFile("join_1.txt")
    val dataStream2 = env.readTextFile("join_2.txt")
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    //使用样例类StockTransaction封装获取的数据


  }
}

