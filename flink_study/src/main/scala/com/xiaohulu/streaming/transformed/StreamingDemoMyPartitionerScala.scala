package com.xiaohulu.streaming.transformed

import com.xiaohulu.streaming.customsource.MyNoParallelSourceScala
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
/**
  * Created by xuwei.tech on 2018/10/23.
  */
object StreamingDemoMyPartitionerScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    //隐式转换

    val text = env.addSource(new MyNoParallelSourceScala)

    //把long类型的数据转成tuple类型
    val tupleData = text.map(line => {
      Tuple1(line) // 注意tuple1的实现方式
    })

    val partitionData = tupleData.partitionCustom(new MyPartitionerScala, 0)

    val result = partitionData.map(line => {
      println("当前线程id：" + Thread.currentThread().getId + ",value: " + line)
      line._1
    })

    result.print().setParallelism(1)

    env.execute("StreamingDemoWithMyNoParallelSourceScala")


  }

}
