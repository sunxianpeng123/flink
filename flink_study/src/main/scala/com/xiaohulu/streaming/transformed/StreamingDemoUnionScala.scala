package com.xiaohulu.streaming.transformed

import com.xiaohulu.streaming.customsource.MyNoParallelSourceScala
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by xuwei.tech on 2018/10/23.
  */
object StreamingDemoUnionScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换

    val text1 = env.addSource(new MyNoParallelSourceScala)
    val text2 = env.addSource(new MyNoParallelSourceScala)

    val unionall: DataStream[Long] = text1.union(text2)

    val sum = unionall.map(line => {
      println("接收到的数据：" + line)
      line
    }).timeWindowAll(Time.seconds(2)).sum(0)

    sum.print().setParallelism(1)

    env.execute("StreamingDemoWithMyNoParallelSourceScala")
  }
}
