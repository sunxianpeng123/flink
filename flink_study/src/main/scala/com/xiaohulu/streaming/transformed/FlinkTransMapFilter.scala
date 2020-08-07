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
object FlinkTransMapFilter {
  def main(args: Array[String]): Unit = {

//  获取运行环境
    val env :StreamExecutionEnvironment =StreamExecutionEnvironment.getExecutionEnvironment

    val mapStream=env.generateSequence(1,10)
    //1   map
    println("map===========")
    val streamMap= mapStream
      .map(_*2)
      .print()
    //2 flatmap
    println("flatmap===========")
    val path ="text01.txt"
    val flatMapStream =env.readTextFile(path)
    flatMapStream
      .flatMap(_.split(" "))
        .print()

    //3 filter
    println("filter===========")
    val filterStream=env.generateSequence(1,10)
    filterStream
      .filter(_==1)
      .print()






    env.execute("FlinkSource01")


  }
  case class WordWithCount(word:String,count:Long)
}

