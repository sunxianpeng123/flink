package com.xiaohulu

import java.util.regex.Pattern

import scala.util.matching.Regex

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/21
  * \* Time: 15:23
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object App {
  def main(args: Array[String]): Unit = {
    val str1 = "满189元,省1.00元"
    val pattern0 = "Scala".r
    val pattern1 = new Regex("(S|s)cala")
    val pattern2 = "^[A-Za-z0-9,]{1,64}".r
    val pattern3 = new Regex("abl[ae]\\d+")
    val str = "Scala, hello scala, ablaw is able123 and cool 11112222"
//    println(pattern0.findAllIn(str).next())
//    println(pattern1.findAllIn(str).mkString(","))
//    println(pattern1.replaceAllIn(str, "Java"))
//    println(pattern2.findAllIn(str).next())
//    println(pattern3.findAllIn(str).mkString(":"))

    val pattern = "[\\d]+\\.*[\\d]*".r
    val t = pattern.findAllIn(str1)
    while (t.hasNext){
      println("----" + t.next())
    }
  }
}

