package com.xiaohulu.datasetapi

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/15
  * \* Time: 17:33
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object Iterationed {
  import org.apache.flink.api.scala.extensions._
  import org.apache.flink.api.scala._
  import org.apache.flink.streaming.api.scala.extensions._
  def main(args: Array[String]): Unit = {
    val env  = ExecutionEnvironment.getExecutionEnvironment
    println("Bulk Iteration =================================")
//    以下示例迭代地估计数量Pi。目标是计算落入单位圆的随机点数。在每次迭代中，挑选一个随机点。如果此点位于单位圆内，增加计数。然后估计 Pi 作为结果计数除以迭代次数乘以4。
    val initial = env.fromElements(0)

    val count = initial.iterate(10000) { iterationInput: DataSet[Int] =>
      val result = iterationInput.map { i =>
        val x = Math.random()
        val y = Math.random()
        i + (if (x * x + y * y < 1) 1 else 0)
      }
      result
    }

    val result = count map { c => c / 10000.0 * 4 }

    result.print()

    println("Delta Iteration =================================")
//    val initialSolutionSet: DataSet[(Long, Double)] = // [...]
//
//    val initialWorkset: DataSet[(Long, Double)] = // [...]
//
//    val maxIterations = 100
//    val keyPosition = 0
//
//    val result1 = initialSolutionSet.iterateDelta(initialWorkset, maxIterations, Array(keyPosition)) {
//      (solution, workset) =>
//        val candidateUpdates = workset.groupBy(1).reduceGroup(new ComputeCandidateChanges())
//        val deltas = candidateUpdates.join(solution).where(0).equalTo(0)(new CompareChangesToCurrent())
//
//        val nextWorkset = deltas.filter(new FilterByThreshold())
//
//        (deltas, nextWorkset)
//    }
//
//    result1.writeAsCsv(outputPath)




    //    env.execute("Iterative Pi Example")

  }
}

