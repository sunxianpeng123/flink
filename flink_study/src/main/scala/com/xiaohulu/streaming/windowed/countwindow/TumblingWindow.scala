package com.xiaohulu.streaming.windowed.countwindow

import com.xiaohulu.streaming.customsource.MyNoParallelSourceScala
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/16
  * \* Time: 10:52
  * \* To change this template use File | Settings | File Templates.
  * \* Description: CountWindow根据窗口中相同key元素的数量来触发执行，执行时只计算元素数量达到窗口大小的key对应的结果。
  * CountWindow根据窗口中相同key元素的数量来触发执行，执行时只计算元素数量达到窗口大小的key对应的结果。
  * \*/
object TumblingWindow {
  import org.apache.flink.api.scala._
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //隐式转换
    import org.apache.flink.api.scala._
    val stream: DataStream[(String, Long)] = env.addSource(new CountWindowSource)

    /**
      * CountWindow 中的滚动窗口（Tumbling Windows）
      * 将数据依据固定的窗口长度对数据进行切分。
      */
    val streamKeyBy: KeyedStream[(String, Long), Tuple] = stream.keyBy(0)
    //注意：CountWindow的window_size 指的是相同key的元素的个数，不是输入的所有元素的总数。
    val streamWindow: DataStream[(String, Long)] = streamKeyBy.countWindow(1)
      .reduce((item1, item2) => (item1._1, item1._2 + item2._2))

    streamWindow.print()

    env.execute("TimeAndWindow")
  }

}
