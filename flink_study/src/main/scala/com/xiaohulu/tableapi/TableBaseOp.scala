//package com.xiaohulu.tableapi
//
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.table.api.scala.StreamTableEnvironment
//import org.apache.flink.table.api.{Table, TableEnvironment}
//import org.apache.flink.table.catalog.ExternalCatalog
//import org.apache.flink.table.sinks.TableSink
//import org.apache.flink.table.sources.TableSource
//
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: sunxianpeng
//  * \* Date: 2019/9/24
//  * \* Time: 19:03
//  * \* To change this template use File | Settings | File Templates.
//  * \* Description:
//  * \*/
//object TableBaseOp {
//  def main(args: Array[String]): Unit = {
//    // 对于批处理程序来说使用 ExecutionEnvironment 来替换 StreamExecutionEnvironment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    // 创建一个TableEnvironment
//    // 对于批处理程序来说使用 BatchTableEnvironment 替换 StreamTableEnvironment
//    val  tableEnv =StreamTableEnvironment.create(env)
//    // 注册一个 Table
//    val table:Table = null
//    val tableSource: TableSource[_] = null
//    val externalCatalog: ExternalCatalog = null
//    tableEnv.createTemporaryView("",table)
//    tableEnv.registerTable("table1",table)            // 或者
//    tableEnv.registerTableSource("table2",tableSource)     // 或者
//    tableEnv.registerExternalCatalog("extCat",externalCatalog)
//    // 从Table API的查询中创建一个Table
//    val tapiResult = tableEnv.scan("table1").select("")
//    // 从SQL查询中创建一个Table
//    val sqlResult  = tableEnv.sqlQuery("SELECT ... FROM table2 ... ")
//    // 将Table API 种的结果 Table 发射到TableSink中 , SQL查询也是一样的
//    val sink: TableSink[String] = null
//    tapiResult.writeToSink(sink)
//
//    // 执行
//    env.execute();
//
//  }
//}
//
