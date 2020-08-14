package com.xiaohulu.streaming.windowed.watermark

import java.text.SimpleDateFormat
import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

/**
  * Watermark 案例
  * Created by xuwei.tech
  */
object WatermarkDemo {

  def main(args: Array[String]): Unit = {
    val port = 9000
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(5000) //默认为200ms，周期性watermark的时间间隔
    env.setParallelism(1)

    val seq = Seq(("hello", 1553503211000L), ("hello", 1553503212001L), ("hello", 1553503213002L), ("hello", 1553503214003L), ("hello", 1553503215004L), ("hello", 1553503216005L))

    //    val stream = env.socketTextStream("hadoop100",port,'\n')
    val stream = env.fromCollection(seq).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(3))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[(String, Long)] {
            override def extractTimestamp(element: (String, Long), recordTimestamp: Long) = element._2
          }
        )
    )



    val result = stream
      .map(e=>(e._1,1L))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .reduce((x, y) => (x._1, x._2 + y._2))

    result.print()

    env.execute("Watermark Test Demo")
  }
}
