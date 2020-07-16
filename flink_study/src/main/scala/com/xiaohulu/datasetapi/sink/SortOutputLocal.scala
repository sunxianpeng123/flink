package com.xiaohulu.datasetapi.sink

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/15
  * \* Time: 17:30
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 本地排序输出
  * \*/
object SortOutputLocal {
  def main(args: Array[String]): Unit = {
    /**
      * 可以使用元组字段位置（field position）或字段表达式（field expression）在指定字段上对数据接收器的输出进行本地排序。这适用于每种输出格式。
      * 尚不支持全局排序的输出。
      */

//    val tData: DataSet[(Int, String, Double)] = // [...]
//    val pData: DataSet[(BookPojo, Double)] = // [...]
//    val sData: DataSet[String] = // [...]
//
//    // sort output on String field in ascending order
//      tData.sortPartition(1, Order.ASCENDING).print()
//
//    // sort output on Double field in descending and Int field in ascending order
//    tData.sortPartition(2, Order.DESCENDING).sortPartition(0, Order.ASCENDING).print()
//
//    // sort output on the "author" field of nested BookPojo in descending order
//    pData.sortPartition("_1.author", Order.DESCENDING).writeAsText(...)
//
//    // sort output on the full tuple in ascending order
//    tData.sortPartition("_", Order.ASCENDING).writeAsCsv(...)
//
//    // sort atomic type (String) output in descending order
//    sData.sortPartition("_", Order.DESCENDING).writeAsText(...)

  }
}

