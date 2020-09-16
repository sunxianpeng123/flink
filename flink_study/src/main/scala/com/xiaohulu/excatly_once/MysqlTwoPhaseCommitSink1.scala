//package com.xiaohulu.excatly_once
//
//import java.sql.Connection
//
//import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction
//
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: sunxianpeng
//  * \* Date: 2020/9/16
//  * \* Time: 16:11
//  * \* To change this template use File | Settings | File Templates.
//  * \* Description:
//  * 自定义kafka to mysql，继承TwoPhaseCommitSinkFunction,实现两阶段提交。
//  * 功能：保证kafak to mysql 的Exactly-Once
//  * \*/
//class MysqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction[(String,String,String,String), Connection, Void] {
//
//}
//
