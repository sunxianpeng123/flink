package com.xiaohulu.udf

import org.apache.flink.table.functions.ScalarFunction

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/28
  * \* Time: 13:53
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
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
