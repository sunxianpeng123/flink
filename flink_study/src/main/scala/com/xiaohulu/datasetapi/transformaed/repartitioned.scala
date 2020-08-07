package com.xiaohulu.datasetapi.transformaed

import org.apache.flink.api.common.operators.Order
import org.apache.flink.core.fs.FileSystem.WriteMode

import scala.collection.mutable
import scala.util.Random

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/15
  * \* Time: 16:49
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object repartitioned {
  import org.apache.flink.api.scala.extensions._
  import org.apache.flink.api.scala._
  import org.apache.flink.streaming.api.scala.extensions._
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //TODO partitionByHash
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Hi"))
    data.+=((2, 2L, "Hello"))
    data.+=((3, 2L, "Hello world"))
    data.+=((4, 3L, "Hello world, how are you?"))
    data.+=((5, 3L, "I am fine."))
    data.+=((6, 3L, "Luke Skywalker"))
    data.+=((7, 4L, "Comment#1"))
    data.+=((8, 4L, "Comment#2"))
    data.+=((9, 4L, "Comment#3"))
    data.+=((10, 4L, "Comment#4"))
    data.+=((11, 5L, "Comment#5"))
    data.+=((12, 5L, "Comment#6"))
    data.+=((13, 5L, "Comment#7"))
    data.+=((14, 5L, "Comment#8"))
    data.+=((15, 5L, "Comment#9"))
    data.+=((16, 6L, "Comment#10"))
    data.+=((17, 6L, "Comment#11"))
    data.+=((18, 6L, "Comment#12"))
    data.+=((19, 6L, "Comment#13"))
    data.+=((20, 6L, "Comment#14"))
    data.+=((21, 6L, "Comment#15"))
    val collection = env.fromCollection(Random.shuffle(data))
    println("partitionByHash===============================")
//    根据给定的 key 对数据集做 hash 分区。可以是 position keys，expression keys 或者 key selector functions。
    val unique1 = collection.partitionByHash(1).mapPartition{
      line =>
        line.map(x => (x._1 , x._2 , x._3))
    }
    unique1.writeAsText("hashPartition", WriteMode.OVERWRITE)

    println("Range-Partition===============================")
//    根据给定的 key 对一个数据集进行 Range 分区。可以是 position keys，expression keys 或者 key selector functions。
    val unique2 = collection.partitionByRange(x => x._1).mapPartition(line => line.map{
      x=>
        (x._1 , x._2 , x._3)
    })
    unique2.writeAsText("rangePartition", WriteMode.OVERWRITE)

    println("sortPartition===============================")
//    根据指定的字段值进行分区的排序；
    val ds = env.fromCollection(Random.shuffle(data))
    val result = ds
      .map { x => x }.setParallelism(2)
      .sortPartition(1, Order.DESCENDING)//第一个参数代表按照哪个字段进行分区
      .mapPartition(line => line)
      .collect()
    println(result)

    println("Custom Partitioning===============================")
//    val in: DataSet[(Int, String)] = // [...]
//    val result = in.partitionCustom(partitioner: Partitioner[K], key)
    println("Rebalance ===============================")
//    val in: DataSet[String] = // [...]
//    val out = in.rebalance().map { ... }



    //      手动指定数据分区。此方法仅适用于单个字段的 key。
//    env.execute()

  }
}

