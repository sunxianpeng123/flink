package com.xiaohulu.control

import java.net.{InetAddress, URL}

import com.xiaohulu.tools.PropertyUtil

import scala.collection.mutable.ArrayBuffer

;

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2018/8/17
  * \* Time: 10:34
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object handler {
  def propertyUtilInit(): Unit = {
    var files = System.getProperty("spark.files")
    println("files ===== " + files)
    if (files == null || files.isEmpty) {
      println(1)
      files = System.getProperty("spark.yarn.dist.files")
    }
    // 上面 都没匹配上 证明是 本地测试跑
    if (files == null|| files.isEmpty) {
      //本地 resource
      println(3)
      val path = this.getClass.getClassLoader.getResource("db_cs.properties").getPath
      files = path
    }
    if (files == null || files.isEmpty) {
      println(4)
      //PropertyUtil.load("/work/spark_task/config/mongodb_douyin_tool.properties")
    } else {
      println(5)
      if (files.startsWith("file:")) {
        println(6)
        PropertyUtil.load(files.substring("file:".length))
        println(PropertyUtil.load(files.substring("file:".length)))
      } else if (files.startsWith("http")) {
        println(7)
        val url = new URL(files);
        PropertyUtil.load(url)
      } else {
        println(8)
        PropertyUtil.load(files)
      }
    }
  }

}

