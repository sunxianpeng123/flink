package com.xiaohulu.streaming.transformed

import com.xiaohulu.streaming.customsource.MyNoParallelSourceScala
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
/**
  * Created by xuwei.tech on 2018/10/23.
  */
object StreamingDemoConnectScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换

    val text1 = env.addSource(new MyNoParallelSourceScala)
    val text2 = env.addSource(new MyNoParallelSourceScala)

    val text2_str = text2.map("str" + _)

    val connectedStreams = text1.connect(text2_str)

    val result = connectedStreams.map(line1 => {
      line1
    }, line2 => {
      line2
    })
    result.print().setParallelism(1)

    env.execute("StreamingDemoWithMyNoParallelSourceScala")

  }

}
