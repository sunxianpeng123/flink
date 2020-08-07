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
object FlinkTransUnion {
  def main(args: Array[String]): Unit = {

//  获取运行环境
    val env :StreamExecutionEnvironment =StreamExecutionEnvironment.getExecutionEnvironment


    val path1 ="text01.txt"
    val stream1 =env.readTextFile(path1).flatMap(_.split(" "))

    val path2 ="text02.txt"
    val stream2 =env.readTextFile(path2).flatMap(_.split(" "))



//comap  coflatmap
    val unionStreams= stream1.union(stream2)

    unionStreams.print()

    env.execute("FlinkSource01")
  }
}

