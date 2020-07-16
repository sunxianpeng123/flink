package com.xiaohulu.streaming.datasource

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/9/4
  * \* Time: 12:11
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object FlinkSelfDataSource {
  def main(args: Array[String]): Unit = {
    val  env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new SourceFromMySql).print()
    env.execute("Flink add self data source")
  }
}

class  SourceFromMySql extends RichSourceFunction[Student]{
  var ps:PreparedStatement = _
  var connection:Connection = _
  /**
    * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
    * @param parameters
    * @throws Exception
    */
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connection = this.getConnection()
    val sql = "select * from student;"
    ps = this.connection.prepareStatement(sql)
  }

  def getConnection():Connection= {
    var con :Connection = null
    try {
      Class.forName("com.mysql.jdbc.Driver")
      con = DriverManager.getConnection("jdbc:mysql://192.168.120.158:3306/test_sun?useUnicode=true&characterEncoding=UTF-8", "root", "1qaz@WSX3edc")
    } catch  {
      case  e:Exception =>System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage)
    }
    con
  }

  override def cancel() = {}
  /**
    * DataStream 调用一次 run() 方法用来获取数据
    * @param sourceContext
    */
  override def run(sourceContext: SourceFunction.SourceContext[Student]) = {
    val  resultSet:ResultSet = ps.executeQuery();
    while (resultSet.next()) {
      val  student = new Student
      student.id = resultSet.getInt("id")
      student.name = resultSet.getString("name").trim()
      student.password = resultSet.getString("password").trim()
      student.age = resultSet.getInt("age")
      sourceContext.collect(student)
    }
  }
}
class Student {
  var id = 0
  var name: String = null
  var password: String = null
  var age = 0
  override def toString = s"Student($id, $name, $password, $age)"
}