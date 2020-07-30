package com.xiaohulu.tableapi

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/4/4
  * \* Time: 16:16
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 表转化成流
  * \*/
object Table2Stream {
  def main(args: Array[String]): Unit = {
//  获取运行环境
    val env :StreamExecutionEnvironment =StreamExecutionEnvironment.getExecutionEnvironment
    //获取table
      val  tableEnv =StreamTableEnvironment.create(env)

    //读取数据源
    val path ="text01.txt"
    val stream =env.readTextFile(path)
//    map
    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.scala._
    val mapStream = stream.flatMap(_.split(" ")).map(word=>WordWithCount(word, 1L))
    //将DataStream转化成Table
    val table = tableEnv.fromDataStream(mapStream)
    //注册表，表名为：word_count
    tableEnv.registerTable("word_count",table)
    //获取表中所有信息
    val sql = "select * from word_count "
    val rs:Table = tableEnv.sqlQuery(sql)
    val rsStream:DataStream[(String,Long)] = rs
      //选取 word 列
      .select("word",'count)
      //将表转化成DataStream
      .toAppendStream[(String,Long)]

    rsStream.print()
    env.execute("flinkSQL")
  }
  case class WordWithCount(word:String,count:Long)
}

