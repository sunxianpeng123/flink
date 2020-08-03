//package com.xiaohulu
//
//import org.apache.flink.api.scala._
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//
//import scala.collection.mutable
////import org.apache.flink.configuration.Configuration
////import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.table.api.TableEnvironment
//
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: sunxianpeng
//  * \* Date: 2019/9/24
//  * \* Time: 17:41
//  * \* To change this template use File | Settings | File Templates.
//  * \* Description:
//  * \*/
//
//object HelloWord {
//
//  def main(args: Array[String]): Unit = {
//    // 测试数据
//    val data = Seq("Flink", "Bob", "Bob", "something", "Hello", "Flink", "Bob")
//
//    // Stream运行环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val  tEnv = StreamTableEnvironment.create(env)
//    println(tEnv)
//    // 最简单的获取Source方式
//    val source = env.fromCollection(data).toTable(tEnv, 'word)
//
//    // 单词统计核心逻辑
//    val result = source
//      .groupBy('word) // 单词分组
//      .select('word, 'word.count) // 单词统计
//
//    //    result.select("word")
//    // 自定义Sink
//    val sink = new RetractSink
//    // 计算结果写入sink
//    result.toRetractStream[(String, Long)].addSink(sink)
//
//    env.execute
//  }
//}
//
//class RetractSink extends RichSinkFunction[(Boolean, (String, Long))] with Serializable {
//  private var resultSet: mutable.Set[(String, Long)] = _
//
//  override def open(parameters: Configuration): Unit = {
//    // 初始化内存存储结构
//    resultSet = new mutable.HashSet[(String, Long)]
//  }
//  override def invoke(v: (Boolean, (String, Long)), context: SinkFunction.Context[_]): Unit = {
//    if (v._1) resultSet.add(v._2) // 计算数据
//    else resultSet.remove(v._2) // 撤回数据
//  }
//
//  override def close(): Unit = {
//    // 打印写入sink的结果数据
//    resultSet.foreach(println)
//  }
//}