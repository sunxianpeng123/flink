package com.xiaohulu.datasetapi

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/15
  * \* Time: 15:51
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object datasource {
  def main(args: Array[String]): Unit = {
    val env  = ExecutionEnvironment.getExecutionEnvironment

    /**
      * file
      */
    // 从本地文件系统读
    val localLines = env.readTextFile("file:///path/to/my/textfile")
    //   // 从本地文件系统读
    val hdfsLines = env.readTextFile("hdfs://nnHost:nnPort/path/to/my/textfile")
    /**
      * csv
      */
    //    // read a CSV file with three fields(假设有三列) 读取CSV文件
    //    val csvInput = env.readCsvFile[(Int, String, Double)]("hdfs:///the/CSV/file")
    //    // read a CSV file with five fields, taking only two of them
    //    // 读取CSV文件
    //    val csvInput = env.readCsvFile[(String, Double)]("hdfs:///the/CSV/file", includedFields = Array(0, 3)) // take the first and the fourth field
    //    // CSV input can also be used with Case Classes,读取CSV映射为一个scala类
    //    case class MyCaseClass(str: String, dbl: Double)
    //    val csvInput = env.readCsvFile[MyCaseClass]("hdfs:///the/CSV/file", includedFields = Array(0, 3)) // take the first and the fourth field
    //    // read a CSV file with three fields into a POJO (Person) with corresponding fields
    //    val csvInput = env.readCsvFile[Person]("hdfs:///the/CSV/file", pojoFields = Array("name", "age", "zipcode"))

    /**
      * collection
      */
    //    // create a set from some given elements从输入字符创建
    //    val values = env.fromElements("Foo", "bar", "foobar", "fubar")
    //
    //    // generate a number sequence从输入字符创建
    //    val numbers = env.generateSequence(1, 10000000)
    //
    //    // read a file from the specified path of type SequenceFileInputFormat
    //    val tuples = env.readSequenceFile(classOf[IntWritable], classOf[Text], "hdfs://nnHost:nnPort/path/to/file")

    //    //从关系型数据库读取
    //    val dbData =
    //      env.createInput(JDBCInputFormat.buildJDBCInputFormat()
    //        .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
    //        .setDBUrl("jdbc:derby:memory:persons")
    //        .setQuery("select name, age from persons")
    //        .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
    //        .finish());
  }
}

