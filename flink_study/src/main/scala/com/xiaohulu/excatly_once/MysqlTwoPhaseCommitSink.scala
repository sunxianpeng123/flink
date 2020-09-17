package com.xiaohulu.excatly_once

import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager, Timestamp}
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Date

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/9/16
  * \* Time: 16:11
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * 自定义kafka to mysql，继承TwoPhaseCommitSinkFunction,实现两阶段提交。
  * 功能：保证kafak to mysql 的Exactly-Once
  * \*/

import org.apache.flink.streaming.api.scala._

class MysqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction[ObjectNode, Connection, Void](
  createTypeInformation[Connection].createSerializer(new ExecutionConfig),
  createTypeInformation[Void].createSerializer(new ExecutionConfig)) {
  /**
    * 1、获取连接，开启手动提交事物（getConnection方法中）
    *
    * @return
    */
  override def beginTransaction(): Connection = {
    val url = s"jdbc:mysql://${Config.dbIpW}:${Config.dbPassportW}/test_sun?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false"
    println(url)
    val connection = DBConnectUtil.getConnection(url, Config.dbUserW, Config.dbPasswordW)
    //
    //    Class.forName("com.mysql.jdbc.Driver")
    //    val connection = DriverManager.getConnection("jdbc:mysql://192.168.120.158:3306/test_sun?useUnicode=true&characterEncoding=UTF-8&useSSL=false", "root", "1qaz@WSX3edc")
    System.err.println("start beginTransaction......." + connection)
    connection
  }

  /**
    * 2、预提交，这里预提交的逻辑在invoke方法中
    *
    * @param transaction
    */
  override def preCommit(transaction: Connection) = {
    System.err.println("start preCommit......." + transaction)
  }

  /**
    * 3、执行数据入库操作
    *
    * @param transaction
    * @param value
    * @param context
    */
  override def invoke(transaction: Connection, value: ObjectNode, context: SinkFunction.Context[_]) = {
    println("start invoke.......")
    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
    println("===>date:" + date + " " + value)
    val data = value.get("value")
    val sql = "insert into test_sun.flink_exactly_once_test (platform_id, room_id, from_id, content,update_time) values (?,?,?,?,?)"
    //    println( s"insert into test_sun.flink_exactly_once_test (platform_id, room_id, from_id, content,update_time) values (${value._1}, ${value._2}, ${value._3}, ${value._4},'$date')")
    val ps = transaction.prepareStatement(sql)
    ps.setString(1, data.get("time").toString)
    ps.setString(2, data.get("time").toString)
    ps.setString(3, data.get("xhlid").toString)
    ps.setString(4, data.get("xhlid").toString)
    ps.setString(5, date)
    //执行insert语句
    //ps.executeUpdate
    ps.execute()
    //手动制造异常
    //if (value._1.toInt == 1) System.out.println(1 / 0);
  }

  /**
    * 4、 如果invoke执行正常则提交事物
    *
    * @param transaction
    */
  override def commit(transaction: Connection) = {
    System.err.println("start commit......." + transaction);
    DBConnectUtil.commit(transaction)
  }

  /**
    * 5、如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
    *
    * @param transaction
    */
  override def abort(transaction: Connection) = {
    System.err.println("start abort rollback......." + transaction)
    DBConnectUtil.rollback(transaction)
  }

  //  /**
  //    *
  //    * @param transaction
  //    */
  //  override def recoverAndAbort(transaction: Connection) = {
  //    System.err.println("start abort recoverAndAbort......." + transaction)
  //  }
}

