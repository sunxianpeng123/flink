package com.xiaohulu.streaming.windowed.countwindow

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/16
  * \* Time: 11:31
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object SlidingWindow {

  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[(String, Long)] = env.addSource(new CountWindowSource)
    /**
      * CountWindow 中的滑动窗口（Sliding Windows）
      * 将数据依据固定的窗口长度对数据进行切分。
      */
    val streamKeyBy: KeyedStream[(String, Long), Tuple] = stream.keyBy(0)
    //注意：CountWindow的window_size 指的是相同key的元素的个数，不是输入的所有元素的总数。
    //满足步长，就执行一次，按第一个参数的长度
    val streamWindow: DataStream[(String, Long)] = streamKeyBy.countWindow(2, 1)
      .reduce((item1, item2) => (item1._1, item1._2 + item2._2))

    streamWindow.print()
    env.execute("TimeAndWindow")
  }
}

