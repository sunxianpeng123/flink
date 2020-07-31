package com.xiaohulu.test

import com.xiaohulu.bean.flinkMapBean.GoodsSaleNumBean

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/28
  * \* Time: 18:33
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object TestOther {
  def main(args: Array[String]): Unit = {

    val g = new GoodsSaleNumBean
    println(g.timestamp)


    var num = 100
    var x = num / 9 * 9
    println(x)
  }
}

