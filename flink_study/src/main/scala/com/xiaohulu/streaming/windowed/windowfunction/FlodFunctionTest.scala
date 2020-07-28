package com.xiaohulu.streaming.windowed.windowfunction

import org.apache.flink.api.common.functions.FoldFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/28
  * \* Time: 16:11
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object FlodFunctionTest {
  def main(args: Array[String]): Unit = {
    /**
      * FlodFunction：可以根据定义的规则将外部元素合并到窗口元素中。flink中已经Deprecated警告，且建议使用AggregateFunction代替。
      */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val stream = env.fromCollection(Seq(("qh1", 100L), ("qh1", 200L), ("qh1", 300L)))

    val result = stream.keyBy(0)
      .countWindow(3)
      .fold("qh", new FoldFunction[(String, Long), String]() {
        override def fold(accumulator: String, value: (String, Long)) = accumulator + value._2
      })

    result.print() //2> qh100200300

    env.execute("aggregate function demo")

  }
}

