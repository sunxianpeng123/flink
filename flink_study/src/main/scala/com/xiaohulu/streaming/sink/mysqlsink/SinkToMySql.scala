package com.xiaohulu.streaming.sink.mysqlsink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/9/4
  * \* Time: 13:43
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
class SinkToMySql extends RichSinkFunction[MessageBean]{
  var ps:PreparedStatement = _
  var connection:Connection = _
  /**
    * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
    * @param parameters
    * @throws Exception
    */
  override  def open(parameters:Configuration ) :Unit= {
    super.open(parameters)
    connection = getConnection()
    val sql = "insert into msg(platform_id, room_id, from_id, content) values(?, ?, ?, ?);"
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

  override def close() :Unit= {
    super.close()
    //关闭连接和释放资源
    if (connection != null) {
      connection.close();
    }
    if (ps != null) {
      ps.close();
    }
  }


  /**
    * 每条数据的插入都要调用一次 invoke() 方法
    *
    * @param value
    * @throws Exception
    */
  @throws[Exception]
  def invoke(value: MessageBean): Unit = { //组装数据，执行插入操作
//    println(value.toString)
    ps.setInt(1, value.platform_id.toInt)
    ps.setString(2, value.room_id)
    ps.setString(3, value.from_id)
    ps.setString(4, filterEmoji(value.content))
    ps.executeUpdate
  }

  def filterEmoji(source: String): String = {
    if (source != null && source.length() > 0) {
      source.replaceAll("[\ud800\udc00-\udbff\udfff\ud800-\udfff]", "")//过滤Emoji表情
        .replaceAll("[\u2764\ufe0f]","")//过滤心形符号
    } else {
      source
    }
  }
}
