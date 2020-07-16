package com.xiaohulu.tableapi

import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/9/24
  * \* Time: 19:23
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object TableEnvironmentTest {
  def main(args: Array[String]): Unit = {
    // ***************
    // STREAMING QUERY
    // ***************
    val sEnv1 = StreamExecutionEnvironment.getExecutionEnvironment
    // 为streaming查询创建一个 TableEnvironment
    val sTableEnv1= TableEnvironment.getTableEnvironment(sEnv1)
    // ***********
    // BATCH QUERY
    // ***********
    val bEnv1 = ExecutionEnvironment.getExecutionEnvironment
    // 为批查询创建一个 TableEnvironment
    val bTableEnv1 = TableEnvironment.getTableEnvironment(bEnv1)
    // ***************
    // STREAMING QUERY
    // ***************
    val sEnv2 = StreamExecutionEnvironment.getExecutionEnvironment
    // 为流查询创建一个 TableEnvironment
    val sTableEnv2 = TableEnvironment.getTableEnvironment(sEnv2)
    // ***********
    // BATCH QUERY
    // ***********
    val bEnv2 = ExecutionEnvironment.getExecutionEnvironment
    // 为批查询创建一个 TableEnvironment
    val bTableEnv2= TableEnvironment.getTableEnvironment(bEnv2)
  }
}

