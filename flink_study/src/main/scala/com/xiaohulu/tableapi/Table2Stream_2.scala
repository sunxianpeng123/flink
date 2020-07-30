package com.xiaohulu.tableapi

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/4/4
  * \* Time: 16:16
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
object Table2Stream_2 {
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
        val mapStream = stream.flatMap(_.split(" ")).map(word=>WordWithCount(word, scala.util.Random.nextInt(10).toLong))
        //将DataStream转化成Table
        val table = tableEnv.fromDataStream(mapStream)
        //注册表，表名为：word_count
        tableEnv.registerTable("word_count",table)
        println("===========rename column name=================")
        val word_count = tableEnv.scan("word_count").as('word,'count)
        println("===========select where filter=================")
        val selectWhereFilter = word_count
          .select('word,'count as 'num)
          .where('num !== 1L )
          .where('num !== 2L)
          .filter('num !== 3L)

        val group = word_count.groupBy('word).select('word,'count.sum as 'num)


        val rsStream:DataStream[(String,Long)] = selectWhereFilter
          //选取 word 列
          .select("word",'num)
          //将表转化成DataStream
          .toAppendStream[(String,Long)]

//        val rsStream:RetractStream[(String,Long)] = group//
//          //选取 word 列
//          .select("word",'num)
//          //将表转化成DataStream
//          .toRetractStream[(String,Long)]



        rsStream.print()

        env.execute("flinkSQL")
    }
    case class WordWithCount(word:String,count:Long)
}

