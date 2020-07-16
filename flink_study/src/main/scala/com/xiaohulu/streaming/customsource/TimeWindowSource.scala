package com.xiaohulu.streaming.customsource

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/16
  * \* Time: 17:12
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
class TimeWindowSource  extends SourceFunction[(String,Long)]{
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
