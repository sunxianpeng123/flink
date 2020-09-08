package com.xiaohulu.streaming.windowed.timewindow

import java.time.Duration

import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/6/7
  * \* Time: 18:16
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object EventTimeWindowConstantWatermark_1dot11 {
  def main(args: Array[String]): Unit = {
    /**
      * 固定延迟生成水印策略，一个延迟3秒的固定延迟水印，
      */
    //  获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    链接socket获取输入数据
    //    val stream = env.socketTextStream("masters", port, ',')
    val stream = env.fromCollection(Seq(("qh1", 100L), ("qh1", 101L)))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness[(String, Long)](
          Duration.ofSeconds(3)//最长延时3秒
        )
          //上述我们讲了flink自带的两种水印生成策略，但是对于我们使用eventtime语义的时候，我们想从我们的自己的数据中抽取eventtime，这个就需要TimestampAssigner了.
          .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
          override def extractTimestamp(element: (String, Long), recordTimestamp: Long) = element._2 //指定EventTime对应的字段
        })
          // 在某些情况下，由于数据产生的比较少，导致一段时间内没有数据产生，进而就没有水印的生成，导致下游依赖水印的一些操作就会出现问题，
          // 比如某一个算子的上游有多个算子，这种情况下，水印是取其上游两个算子的较小值，如果上游某一个算子因为缺少数据迟迟没有生成水印，就会出现eventtime倾斜问题，导致下游没法触发计算。
          // 所以filnk通过WatermarkStrategy.withIdleness()方法允许用户在配置的时间内（即超时时间内）没有记录到达时将一个流标记为空闲。这样就意味着下游的数据不需要等待水印的到来。
          .withIdleness(Duration.ofMinutes(1))
      )
      .keyBy(_._1)

    //    //指定滚动窗口
    //    val streamWindow = stream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
    //    val streamWindow2 = stream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
    //指定滑动窗口
    //    val streamWindow = stream.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    //指定会话窗口,相邻两条超过指定时间会执行
    val streamWindow = stream.window(EventTimeSessionWindows.withGap(Time.seconds(5)))
    val streamReduce = streamWindow.reduce((item1, item2) => (item1._1, item1._2 + item2._2))

    streamReduce.print()

    env.execute("FlinkSource01")
  }
}

//

