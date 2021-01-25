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
object FlinkTransKeyByReduce {
  def main(args: Array[String]): Unit = {

    //  获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val path = "text01.txt"
    val stream2 = env.readTextFile(path)
      .flatMap(_.split(" "))
      .map(e => (e, 1L))
    //1 keyby
    val keyByStream: KeyedStream[(String, Long), String] = stream2
      //      .keyBy(0)
      .keyBy(_._1)

    //  2 reduce
    val reduceStream: DataStream[(String, Long)] = keyByStream.reduce((x, y) => (x._1, x._2 + y._2))


    reduceStream.print()


    env.execute("FlinkSource01")
  }
}

