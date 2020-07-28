package com.xiaohulu.test

import java.sql.Timestamp

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
//import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/27
  * \* Time: 16:38
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
object StreamTableApi {
  def main(args: Array[String]): Unit = {
    // Get the stream and table environments.
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    // Provide a static data set of the rates history table.
    val ratesHistoryData = new mutable.MutableList[(String, Long)]
    ratesHistoryData.+=(("US Dollar", 102L))
    ratesHistoryData.+=(("Euro", 114L))
    ratesHistoryData.+=(("Yen", 1L))
    ratesHistoryData.+=(("Euro", 116L))
    ratesHistoryData.+=(("Euro", 119L))
    // Create and register an example table using above data set.
    // In the real setup, you should replace this with your own table.
    val ratesHistory = env
      .fromCollection(ratesHistoryData)
      .toTable(tEnv, 'r_currency, 'r_rate, 'r_proctime.proctime)
    tEnv.createTemporaryView("RatesHistory", ratesHistory)

    println('r_proctime.proctime)
    // Create and register TemporalTableFunction.
    // Define "r_proctime" as the time attribute and "r_currency" as the primary key.
    val rates = ratesHistory.createTemporalTableFunction('r_proctime, 'r_currency) // <==== (1)
    tEnv.registerFunction("Rates", rates) // <==== (2)

    val sql = "select r_currency,r_rate,r_proctime from RatesHistory "
    val rs = tEnv.sqlQuery(sql)      //选取 word 列
//      .toAppendStream[(String, Long,Timestamp)]
//      .select("r_currency",'r_rate)
    rs.printSchema()
//    rs.print()




//    env.execute("Flink Anchor Stream Live Schedule")


  }
}

