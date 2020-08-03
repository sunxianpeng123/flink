package com.xiaohulu.test

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

//import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/27
  * \* Time: 17:41
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object UDFTest {
  import org.apache.flink.table.api._
  import org.apache.flink.table.api.bridge.scala._
  import org.apache.flink.streaming.api.scala._
  case class S(name:String)
  case class R(splited:String)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    val seq = List("wang tao", "li ming", "li lei")
    val stream = env.fromCollection(seq).map(e=>S(e))
    val table = tEnv.fromDataStream(stream)
    table.printSchema()

    val table_name = "test_table"
    tEnv.createTemporaryView("test_table", table)
    tEnv.registerFunction("split", new Split())

    // use the function in SQL API
    val t = tEnv.sqlQuery(s"SELECT split(name) as splited FROM $table_name")
    t.printSchema()
    t.toRetractStream[Row].print()


    env.execute("socket window count scala")
  }
}

class Split extends ScalarFunction {
  def eval(name:String) :String =  {
    name.split(" ")(0)
  }
}