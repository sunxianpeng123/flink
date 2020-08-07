package com.xiaohulu.datasetapi.transformaed

import scala.collection.mutable.ListBuffer

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/15
  * \* Time: 16:36
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 将两个 DataSet 连接生成一个新的 DataSet。
  * \*/
object joined {

  import org.apache.flink.api.scala.extensions._
  import org.apache.flink.api.scala._
  import org.apache.flink.streaming.api.scala.extensions._

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data1 = ListBuffer[(Int, String)]()
    data1.append((1, "ruoze"))
    data1.append((2, "jepson"))
    data1.append((3, "xingxing"))

    val data2 = ListBuffer[(Int, String)]()
    data2.append((1, "beijing"))
    data2.append((2, "shanghai"))
    data2.append((4, "hangzhou"))

    val a = env.fromCollection(data1)
    val b = env.fromCollection(data2)

    println("内连接==============================")
    a.join(b) // where 和 equalTo 方法必须要使用
      .where(0) //左边的key，0表示第一列
      .equalTo(0) //右边的key，0表示第一列
      .apply((a, b) => {
      (a._1, a._2, b._2)
    })
      .print()

    println("左连接==============================")
    a.leftOuterJoin(b) // where 和 equalTo 方法必须要使用
      .where(0) //左边的key，0表示第一列
      .equalTo(0) //右边的key，0表示第一列
      .apply((a, b) => {
      if (b == null) { //要判断如果右边没有匹配的，显示什么
        (a._1, a._2, "-")
      } else {
        (a._1, a._2, b._2)
      }
    })
      .print()
    println("右连接==============================")
    a.rightOuterJoin(b) // where 和 equalTo 方法必须要使用
      .where(0) //左边的key，0表示第一列
      .equalTo(0) //右边的key，0表示第一列
      .apply((a, b) => {
      if (a == null) { //要判断如果左边没有匹配的，显示什么
        (b._1, "-", b._2)
      } else {
        (b._1, a._2, b._2)
      }
    })
      .print()
    println("全外连接==============================")
    //
    a.fullOuterJoin(b)
      .where(0)
      .equalTo(0)
      .apply((a, b) => {
        if (a == null) { //全连接 ，左边和右边 都要判读为null的情况
          (b._1, "-", b._2)
        } else if (b == null) {
          (a._1, a._2, "-")
        } else {
          (b._1, a._2, b._2)
        }
      })
      .print()

    println("笛卡尔积==============================")
    //
    val ds1 = List("manlian", "ashenna")
    val ds2 = List("3", "2", "0")
    val left = env.fromCollection(ds1)
    val right = env.fromCollection(ds2)
    left.cross(right).print()
  }
}

