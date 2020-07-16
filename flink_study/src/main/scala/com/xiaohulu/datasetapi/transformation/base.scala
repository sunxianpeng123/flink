package com.xiaohulu.datasetapi.transformation

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/15
  * \* Time: 16:02
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object base {
  import org.apache.flink.api.scala.extensions._
  import org.apache.flink.api.scala._
  import org.apache.flink.streaming.api.scala.extensions._
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val values = env.fromElements("1", "2")
    println("map=================")
    values.map(_.toUpperCase()).print()

    println("flatmap===============")
    values.flatMap(_.split(",")).print()

    println("mappartition==============")
    values.mapPartition(iter => iter.map(_ + " 12")).print()

    println("filter===========")
    values.filter(_.contains("1")).print() //将符合条件的过滤出来

    println("distinct==============")
    //    val input: DataSet[(Int, String, Double)] = // [...]
    //    val output = input.distinct()
    //
    //    // Distinct with field position keys
    //    val input: DataSet[(Int, Double, String)] = // [...]
    //    val output = input.distinct(0,2)
    //
    //    // Distinct with KeySelector function
    //    val input: DataSet[Int] = // [...]
    //    val output = input.distinct {x => Math.abs(x)}
    //
    //    // Distinct with key expression
    //    case class CustomType(aName : String, aNumber : Int) { }
    //
    //    val input: DataSet[CustomType] = // [...]
    //    val output = input.distinct("aName", "aNumber")

    println("reduce==============")
    values.map(_.toInt).reduce(_ + _).print()


  }
}

