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
object FlinkTransSplit {
  def main(args: Array[String]): Unit = {

//  获取运行环境
    val env :StreamExecutionEnvironment =StreamExecutionEnvironment.getExecutionEnvironment

    val path ="text01.txt"
    val stream2 =env.readTextFile(path).flatMap(_.split(" "))


      val streamSplit =stream2.split(
        word=>
       ("java".equals(word)) match {
         case true =>List("java")
         case false => List("other")
       }
      )

    val streamSelect1=streamSplit.select("java")
    val streamSelect2 = streamSplit.select("other")

    streamSelect1.print()




    env.execute("FlinkSource01")
  }
}

