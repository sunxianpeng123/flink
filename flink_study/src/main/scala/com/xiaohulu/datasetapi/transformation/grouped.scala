package com.xiaohulu.datasetapi.transformation

import org.apache.flink.api.scala.extensions._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.util.Collector

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/15
  * \* Time: 16:18
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object grouped {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    println("Pojo 类型,可以根据 KeyExpression 或 KeySelector 分区=================")
    //    class WC(val word: String, val count: Int) {
    //      def this() {
    //        this(null, -1)
    //      }
    //      // [...]
    //    }
    //
    //    val words: DataSet[WC] = // [...]
    //
    //    // Grouped by Key Expression
    //    val wordCounts1 = words.groupBy("word")
    //
    //    // Grouped by KeySelector Function
    //    val wordCounts2 = words.groupBy { _.word }
    println("元组（Tuple）类型，可以根据字段位置分组=================")
    //    val tuples = DataSet[(String, Int, Double)] = // [...]
    //
    //    // 根据元组的第一和第二个字段分组
    //    val reducedTuples = tuples.groupBy(0, 1)
    println("分组并排序=================")
    //    val input: DataSet[(Int, String)] = // [...]
    //    val output = input.groupBy(0).sortGroup(1, Order.ASCENDING)
    println("Reduce========================")
    //    val words: DataSet[WC] = // [...]
    //    val wordCounts = words.groupBy("word").reduce {
    //      (w1, w2) => new WC(w1.word, w1.count + w2.count)
    //    }
    println("ReduceGroup========================")

    val input1 = env.fromElements((1, "a"), (1, "b"), (2, "a"), (3, "c"))
    val output1 = input1.groupBy(0).reduceGroup {
      (in, out: Collector[(Int, String)]) =>
        out.collect(in.reduce((x, y) => (x._1, x._2 + y._2)))
    }
    output1.print()
    // 输出 (3,c),(1,ab),(2,a)
    println("Aggregate_____max,sum,min ========================")
    val input2: DataSet[(Int, String, Double)] = env.fromElements(
      (1, "a", 10d), (1, "b", 20d), (2, "a", 30d), (3, "c", 50d)
    )
    val output2: DataSet[(Int, String, Double)] = input2.groupBy(1).sum(0).max(2)
    output2.print()
    // 输出 (3,a,50.0)
    println("Aggregate______MinBy / MaxBy================")
    val input3: DataSet[(Int, String, Double)] = env.fromElements(
      (1, "b", 20d), (1, "a", 10d), (2, "a", 30d)
    )
    // 按第二个字段分组，取第一、三个字段最小的元组
    val output3: DataSet[(Int, String, Double)] = input3.groupBy(1).minBy(0, 2)
    output3.print()
    // 输出 (1,a,10.0),(1,b,20.0)


  }
}
