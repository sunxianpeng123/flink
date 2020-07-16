package com.xiaohulu.datasetapi.transformation

import org.apache.flink.api.java.aggregation.Aggregations

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/15
  * \* Time: 16:12
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object Aggregate {

  import org.apache.flink.api.scala.extensions._
  import org.apache.flink.api.scala._
  import org.apache.flink.streaming.api.scala.extensions._

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input: DataSet[(Int, String, Double)] = env.fromElements(
      (1, "a", 10d), (1, "b", 20d), (2, "a", 30d), (3, "c", 50d)
    )

    println("sum,min,max===================")
    val output1: DataSet[(Int, String, Double)] = input.aggregate(Aggregations.SUM, 0).aggregate(Aggregations.MIN, 2)
    output1.print()
    // 输出 (7,c,50.0)
    // 简化语法
    val output2: DataSet[(Int, String, Double)] = input.sum(0).min(2)
    output2.print()

    println("MinBy / MaxBy =================")//根据某个字段取出最大值或者最小值
    // 比较元组的第一个字段
    val output3: DataSet[(Int, String, Double)] = input.minBy(0)
    // 输出 (1,a,10.0)
    output3.print()
    // 比较元组的第一、三个字段
    val output4: DataSet[(Int, String, Double)] = input.minBy(0,2)
    // 输出 (1,a,10.0)
    output4.print()


  }
}

