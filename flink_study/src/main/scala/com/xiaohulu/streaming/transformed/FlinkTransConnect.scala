package com.xiaohulu.streaming.transformed

import org.apache.flink.streaming.api.scala._

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/4/4
  * \* Time: 16:16
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
object FlinkTransConnect {
  def main(args: Array[String]): Unit = {

    //  获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1: DataStream[Long] = env.generateSequence(1, 10)

    val path = "text01.txt"
    val stream2: DataStream[String] = env.readTextFile(path).flatMap(_.split(" "))

    //1 comap
    val connectedStreams: ConnectedStreams[Long, String] = stream1.connect(stream2)

    //    connectedStreams.map(item=>item*2,item=>(item,1L)).print()//两个元素的处理是完全独立的
    //    connectedStreams.map(item=>println(item+"___"),item=>println(item))

    // 2 coflatmap
    connectedStreams.flatMap(item => List(item), item => List(item)).print()


    env.execute("FlinkSource01")
  }
}

