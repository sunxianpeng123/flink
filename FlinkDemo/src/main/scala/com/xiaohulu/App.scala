package com.xiaohulu

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.functions.ScalarFunction

//import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/27
  * \* Time: 17:41
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object App {

  case class C(promote_remark_max: String, platform_id: Long)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    链接socket获取输入数据
    val sourceStream = env.addSource(new TimeWindowSource)
    val tEnv = StreamTableEnvironment.create(env)

    tEnv.registerFunction("regexp_extract_all",new RegexpExtractAll)
    val transStream = sourceStream.map(e => C(e._1, e._2)).toTable(tEnv)
    val tbname = "info"
    tEnv.createTemporaryView(tbname,transStream)

    val sql = s"select platform_id,if(promote_remark_max is not null,regexp_extract_all(promote_remark_max),null) as discounts_price from $tbname"

    tEnv.sqlQuery(sql).toAppendStream[(Int,Array[String])].print() //批转流
    env.execute("socket window count scala")


  }
}

class TimeWindowSource extends SourceFunction[(String, Int)] {
  val str = "满5000元,减500.0元#满50000元#减5000.0元"
  var res: (String, Int) = _
  var isRunning = true

  override def run(ctx: SourceContext[(String, Int)]) = {
    while (isRunning) {
      val rand = scala.util.Random.nextInt(26)
      val num = rand % 26
      res = (str, num)
      ctx.collect(res)

      Thread.sleep(1000)
    }

  }

  override def cancel() = {
    isRunning = false
  }
}

/**
  * 自定义UDF
  */
class RegexpExtractAll extends ScalarFunction {

  def eval(promote_remark_max: String): Array[String] = {

    var arr: Array[String] = Array.empty
    val pattern = "[\\d]+\\.*[\\d]*".r
    val group = pattern.findAllIn(promote_remark_max)
    while (group.hasNext) arr :+= group.next()
    if (arr.length < 2) arr = Array("0", "0")
    arr
  }

}