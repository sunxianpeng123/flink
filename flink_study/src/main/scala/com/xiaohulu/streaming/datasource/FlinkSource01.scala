package com.xiaohulu.streaming.datasource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/4/4
  * \* Time: 16:16
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
object FlinkSource01 {
  def main(args: Array[String]): Unit = {
    //  获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val path = "text01.txt"
    val stream = env.readTextFile(path)
    //    val list =List(1,2,3,4)
    //    val stream= env.fromCollection(list)
    //    val stream =env.generateSequence(1,10)
    import org.apache.flink.api.scala._
    val keybyReduceStream = stream.flatMap(_.split(" "))
      .map(w => (w, 1)) //把单词转成（word，1）这种形式
      .keyBy(_._1)
      .reduce((x, y) => (x._1, x._2 + y._2))
    //每个分区
    keybyReduceStream.writeAsText("test_write_txt")
    keybyReduceStream.print()
    env.execute("FlinkSource01")
  }

  case class WordWithCount(word: String, count: Long)

}

