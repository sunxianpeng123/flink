package com.xiaohulu.datasetapi.sink

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/15
  * \* Time: 17:28
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object baseOutput {
  def main(args: Array[String]): Unit = {
    /**
      * Data Sink 从 DataSet 中取出数据保存或者返回。Flink 各种内置的输出格式，在 DataSet 上的算子操作后面调用：

    writeAsText() / TextOutputFormat，将元素以字符串形式写入文件。字符串通过调用每个元素的 toString() 方法获得。

    writeAsCsv(...) / CsvOutputFormat，将元组字段以逗号分隔写入文件。行和字段分隔符是可配置的。每个字段的值来自对象的 toString() 方法。

    print() / printToErr()，在标准输出/标准错误输出中打印每个元素的 toString() 返回值。

    write() / FileOutputFormat，自定义文件输出的方法和基类。支持自定义对象到字节转换。

    output() / OutputFormat，通用输出方法，用于非基于文件的数据接收器。
      */
//    // text data
//    val textData: DataSet[String] = // [...]
//
//    // write DataSet to a file on the local file system
//    textData.writeAsText("file:///my/result/on/localFS")
//
//    // write DataSet to a file on a HDFS with a namenode running at nnHost:nnPort
//    textData.writeAsText("hdfs://nnHost:nnPort/my/result/on/localFS")
//
//    // write DataSet to a file and overwrite the file if it exists
//    textData.writeAsText("file:///my/result/on/localFS", WriteMode.OVERWRITE)
//
//    // tuples as lines with pipe as the separator "a|b|c"
//    val values: DataSet[(String, Int, Double)] = // [...]
//      values.writeAsCsv("file:///path/to/the/result/file", "\n", "|")
//
//    // this writes tuples in the text formatting "(a, b, c)", rather than as CSV lines
//    values.writeAsText("file:///path/to/the/result/file")
//
//    // this writes values as strings using a user-defined formatting
//    values map { tuple => tuple._1 + " - " + tuple._2 }
//      .writeAsText("file:///path/to/the/result/file")

  }
}
