package com.xiaohulu.streaming.transformationed

import com.xiaohulu.streaming.customsource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/8/3
  * \* Time: 18:27
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object StreamingJoin {
  import org.apache.flink.table.api._
  import org.apache.flink.table.api.bridge.scala._
  import org.apache.flink.streaming.api.scala._
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换

    val text1 = env.addSource(new MyNoParallelSourceScala)
    val text2 = env.addSource(new MyNoParallelSourceScala)

    val unionall = text1.union(text2)

    val sum = unionall.map(line=>{
      println("接收到的数据："+line)
      line
    }).timeWindowAll(Time.seconds(2)).sum(0)

    sum.print().setParallelism(1)

    env.execute("StreamingDemoWithMyNoParallelSourceScala")


  }
}

