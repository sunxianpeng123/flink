package com.xiaohulu.streaming.transformed

import com.xiaohulu.streaming.customsource.MyNoParallelSourceScala
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * Created by xuwei.tech on 2018/10/23.
  */
object StreamingDemoFilterScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换

    val text = env.addSource(new MyNoParallelSourceScala2)

    val mapData = text.map(line => {
      println("原始接收到的数据：" + line)
      line
    }).filter(_ % 2 == 0)

    val sum = mapData.map(line => {
      println("过滤之后的数据：" + line)
      line
    }).timeWindowAll(Time.seconds(2)).sum(0)


    sum.print().setParallelism(1)

    env.execute("StreamingDemoWithMyNoParallelSourceScala")


  }

}
class MyNoParallelSourceScala2 extends SourceFunction[Long]{

  var count = 1L
  var isRunning = true

  override def run(ctx: SourceContext[Long]) = {
    while(isRunning){
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }

  }

  override def cancel() = {
    isRunning = false
  }
}