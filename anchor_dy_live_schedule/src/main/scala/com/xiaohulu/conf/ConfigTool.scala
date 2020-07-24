package com.xiaohulu.conf

import java.io.FileInputStream
import java.util.Properties

import com.xiaohulu.mysqlsink.MySqlSinkApp.TRANSACTION_GROUP

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/10/23
  * \* Time: 14:07
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object ConfigTool {
  val properties = new Properties()
  val filePath = "db_stream_dy_live_2parquet.properties"
  properties.load(new FileInputStream(filePath))

  /**
    * 任务名字
    */
  val spark_app_name = properties.getProperty("spark_app_name")
  /**
    * 间隔时间
    */
  val streaming_minutes_interval = properties.getProperty("streaming_minutes_interval")
  /**
    * 本地测试还是现象运行
    */
  val where_to_run = properties.getProperty("where_to_run")
  /**
    * parquet 数据写入路径
    */
  val anchor_basic_path = properties.getProperty("anchor_basic_info_path").trim
  val fans_info_path = properties.getProperty("fans_info_path").trim
  val fans_list_path = properties.getProperty("fans_list_path").trim
  val goods_info_path = properties.getProperty("goods_info_path").trim
  val purchase_intention_path = properties.getProperty("purchase_intention_path").trim
  //todo  淘宝电商数据的 hdfs 存储路径
  val tb_anchor_basic_info_path = properties.getProperty("tb_anchor_basic_info_path").trim
  val tb_goods_info_path = properties.getProperty("tb_goods_info_path").trim
  //todo  快手电商数据的 hdfs 存储路径
  val ks_anchor_basic_info_path = properties.getProperty("ks_anchor_basic_info_path").trim
  val ks_goods_info_path = properties.getProperty("ks_goods_info_path").trim
  /**
    * kafka 相关配置
    */
  //  kafka  config
  //    val platSet=PropertyUtil.getString("platIDS").split(",").map(x=>{s"node-bullet-crawler-$x"}).toSet
   val anchor_basic_info_topics = properties.getProperty("anchor_basic_info_topics").trim
   val fans_info_topics = properties.getProperty("fans_info_topics").split(",").map(_.trim).toSet
   val fans_list_topics = properties.getProperty("fans_list_topics").split(",").map(_.trim).toSet
   val anchor_goods_topics = properties.getProperty("anchor_goods_topics").split(",").map(_.trim).toSet
   val purchase_intention_topics = properties.getProperty("purchase_intention_topics").split(",").map(_.trim).toSet
  //todo  淘宝电商数据的topics
  private val taobao_live_info_topics = properties.getProperty("taobao_live_info_topics").split(",").map(_.trim).toSet
  private val taobao_goods_info_topics = properties.getProperty("taobao_goods_info_topics").split(",").map(_.trim).toSet
  //todo  快手电商数据的topics
  private val kuaishou_live_info_topics = properties.getProperty("kuaishou_live_info_topics").split(",").map(_.trim).toSet
  private val kuaishou_goods_info_topics = properties.getProperty("kuaishou_goods_info_topics").split(",").map(_.trim).toSet

  var topics = anchor_basic_info_topics ++ fans_info_topics ++ fans_list_topics ++ anchor_goods_topics ++ purchase_intention_topics
  topics = topics ++ taobao_live_info_topics ++ taobao_goods_info_topics
  topics = topics ++ kuaishou_live_info_topics ++ kuaishou_goods_info_topics
  //  val topics = taobao_live_info_topics ++ taobao_goods_info_topics

  private val brokers = properties.getProperty("brokers").trim
  private val groupId = properties.getProperty("KafkaGroupId").trim
  val kafkaProps = new Properties()
  kafkaProps.setProperty("bootstrap.servers", brokers)
  kafkaProps.setProperty("group.id", groupId)

}

