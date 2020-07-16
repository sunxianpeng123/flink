package com.xiaohulu.streaming.windowed.countwindow

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
  * 创建自定义并行度为1的source
  *
  * 实现从1开始产生递增数字
  *
  * Created by xuwei.tech on 2018/10/23.
  */
class CountWindowSource extends SourceFunction[(String,Long)]{

  val str = "abcdefghijklmnopqrstuvwxyz"
  var res :(String,Long) = _
  var isRunning = true

  override def run(ctx: SourceContext[(String,Long)]) = {
    while(isRunning){
      val rand = scala.util.Random.nextInt(26)
      val num = rand % 26
      val c = str.charAt(num).toString

      res = (c, num)
      ctx.collect(res)

      Thread.sleep(1000)
    }

  }

  override def cancel() = {
    isRunning = false
  }
}
