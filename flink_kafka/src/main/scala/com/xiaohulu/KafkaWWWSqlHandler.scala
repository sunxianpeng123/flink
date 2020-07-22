package com.xiaohulu

import java.net.URI
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{ArrayList, Date, List, Properties}

import com.google.common.collect.Lists
import com.google.gson.{Gson, JsonParser}
import com.xiaohulu.bean.{WWWBean, WWWBeanList}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.Window

import scala.collection.mutable.ListBuffer

object KafkaWWWSqlHandler {

  private val KAFKA_BROKER = "Kafka-01:9092"
  //private val KAFKA_BROKER = "219.129.216.209:9092"
  private val TRANSACTION_GROUP = "com.xhl.flink05sxp"

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    val source = new URI("file:///tmp/checkpoint")
    //    env.setStateBackend(new FsStateBackend(source));
    //    env.enableCheckpointing(1000*60)
    //    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new MemoryStateBackend(1000, false))
    //env.setStateBackend( new RocksDBStateBackend(new URI("hdfs:///192.168.120.160/check_point/")))


    val tenv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure Kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    val consumer = new FlinkKafkaConsumer[String]("www_request_log", new SimpleStringSchema(), kafkaProps)
    val transaction = env.addSource(consumer)
    println("start data process")

    val orginDataStream = transaction.flatMap(line => {
      var dbArr: Array[WWWBean] = null
      val jsonParse = new JsonParser()
      val gs = new Gson()
      try {
        val je = jsonParse.parse(line)
        if (je.isJsonArray) {
          dbArr = gs.fromJson(je, classOf[Array[WWWBean]])
        } else {
          val dbb = gs.fromJson(je, classOf[WWWBean])
          dbArr = new Array[WWWBean](1)
          dbArr(0) = dbb
        }
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
      dbArr
    }
    )


    val dataStream = orginDataStream.map(x => {
      x.row_time = x.time

      x
    })
      .assignAscendingTimestamps(_.row_time * 1000)


    tenv.createTemporaryView("request_info", dataStream, 'url, 'xhlid, 'row_time.rowtime)
    //
    //    var hop_parma="INTERVAL '1' MINUTE, INTERVAL '5' MINUTE"
    //    val resultTable= tenv.sqlQuery("select  HOP_START(row_time,"+hop_parma+") start_time," +
    //      " HOP_END(row_time, "+hop_parma+") end_time," +
    //      "xhlid,count(distinct url) as viewPageCount ,count(*) as p from request_info  " +
    //      " GROUP BY HOP( row_time  ,"+hop_parma+"), xhlid   ")
    // val resultDataStream=tenv.toRetractStream[(Timestamp,Timestamp,String,Long,Long)](resultTable)
    var hop_parma2 = "INTERVAL '1' MINUTE"
    val resultTable2 = tenv.sqlQuery("select  TUMBLE_START(row_time," + hop_parma2 + ") start_time," +
      " TUMBLE_END(row_time, " + hop_parma2 + ") end_time," +
      "xhlid,count(distinct url) as viewPageCount ,count(*) as p from request_info  " +
      " GROUP BY TUMBLE( row_time  ," + hop_parma2 + "), xhlid   ")


    val resultDataStream2 = tenv.toRetractStream[(Timestamp, Timestamp, String, Long, Long)](resultTable2)
    // val resultDataStream1200= resultDataStream.filter(x=>x._2._5>6000)

    val resultDataStream100 = resultDataStream2.filter(x => x._2._5 > 1)

    val resultDataStream200 = resultDataStream2.filter(x => x._2._5 > 2)


    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("120.92.84.191", 31203, "http"))
    val dateFmt = new SimpleDateFormat("yyyyMMdd")

    val esSinkBuilder = new ElasticsearchSink.Builder[WWWBeanList](
      httpHosts,
      new ElasticsearchSinkFunction[WWWBeanList] {

        def process(element: WWWBeanList, ctx: RuntimeContext, indexer: RequestIndexer) {
          try {
            val json = new java.util.HashMap[String, Any]
            element.buffer.foreach(e => {
              json.put("ip", e.ip)
              json.put("time", e.time)
              json.put("url", e.url)
              json.put("xhlid", e.xhlid)
              val inputInfo = new Gson().toJson(e.input)
              json.put("param_info", inputInfo)
              val indexDate = dateFmt.format(new Date(e.time * 1000))
              val rqst: IndexRequest = Requests.indexRequest
                .index("official_website_" + indexDate)
                .source(json)
              indexer.add(rqst)
            })
          } catch {
            case e: Exception => println(e.getMessage)
          }
        }

      }
    )

    resultDataStream200.map(x => {
      print("resultDataStream200===========================")
      (x._2._3, 300)
    }).print()

    resultDataStream100.map(x => {
      print("resultDataStream100===========================")
      (x._2._3, 120)
    }).print()

    //    resultDataStream200.map(x => {
    //      (x._2._3, 300)
    //    }).addSink(new CouponSink())
    //    resultDataStream100.print()
    //    resultDataStream100.map(x => {
    //      (x._2._3, 120)
    //    }).addSink(new CouponSink())
    //
    //    val batchDs = orginDataStream.assignAscendingTimestamps(_.time * 1000).timeWindowAll(Time.seconds(60)).process(new BatchWindowFunction)
    //    esSinkBuilder.setBulkFlushMaxActions(1)
    //    esSinkBuilder.setBulkFlushMaxSizeMb(50)
    //    esSinkBuilder.setBulkFlushInterval(3000)
    //    batchDs.addSink(esSinkBuilder.build)
    println("down")
    env.execute()
  }


}
