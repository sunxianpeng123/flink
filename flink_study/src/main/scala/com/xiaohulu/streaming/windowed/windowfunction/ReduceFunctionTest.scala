package com.xiaohulu.streaming.windowed.windowfunction

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/28
  * \* Time: 15:43
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
object ReduceFunctionTest {
  def main(args: Array[String]): Unit = {
    /**
      * ReduceFunction：对输入的两个相同类型的数据元素按照指定的计算方法进行聚合，然后输出一个类型相同的结果元素。
      */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val stream = env.fromCollection(Seq(("qh1", 100L), ("qh1", 101L)))

    val result = stream.keyBy(0)
      .countWindow(2, 2)
      .reduce(
        new ReduceFunction[(String, Long)]() {
          override def reduce(value1: (String, Long), value2: (String, Long)): (String, Long) = {
            (value1._1, value1._2 + value2._2)
          }
        }
      )

    result.print() //2> (qh1,201)


    env.execute(" reduce function demo")
  }
}

