package com.other

import java.net.URI
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import com.xiaohulu.bean.{AnchorResultBean, DataBean, FansInfoResultBean, FansListResultBean, GoodsInfoBean, GoodsResultBean}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import com.google.gson.{Gson, JsonParser}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

import scala.collection.JavaConverters._
object KafkaSqlHandler {

  private val KAFKA_BROKER = "Kafka-01:9092"
  //private val KAFKA_BROKER = "219.129.216.209:9092"
  private val TRANSACTION_GROUP = "com.wugenqiang.flinksxp"

  def main(args : Array[String]){
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = new URI("file:///tmp/checkpoint")
    env.setStateBackend(new FsStateBackend(source));
    env.enableCheckpointing(1000*60)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //env.setStateBackend(new MemoryStateBackend(1000, false))
    //env.setStateBackend( new RocksDBStateBackend(new URI("hdfs:///192.168.120.160/check_point/")))
    val  tenv =StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure Kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    val transaction = env.addSource(new FlinkKafkaConsumer[String]("crawler-data-scene-goods-infos-71", new SimpleStringSchema(), kafkaProps))



    val  dataStream= transaction.flatMap(line => {

      var dbArr: Array[DataBean] = null
      val jsonParse = new JsonParser()
      val gs = new Gson()
      try {
        val je = jsonParse.parse(line)

        if (je.isJsonArray) {
          dbArr = gs.fromJson(je, classOf[Array[DataBean]])
        } else {
          val dbb = gs.fromJson(je, classOf[DataBean])
          dbArr = new Array[DataBean](1)
          dbArr(0) = dbb
        }
      }
        catch {
          case e: Exception => println(e.getMessage)
        }
      dbArr
     }
    ).filter(x=>x!=null).flatMap(x=>{x.item}).flatMap(x=>{
      var goodsInfoList:Array[GoodsInfoBean] = Array.empty
      x.goodsInfoList.foreach(g=>{
        g.time=x.time
        g.room_id=x.uid
        g.live_id=x.liveId
        g.row_time=x.time
        g.eventAccTime= new Timestamp(x.time*1000)
        goodsInfoList :+= g
      })
      goodsInfoList
    })
   .assignAscendingTimestamps(_.row_time*1000)


    //val dataTable =tenv.fromDataStream(dataStream)
    //tenv.createTemporaryView("goods_info",dataTable)
    //tenv.createTemporaryView("goods_info", dataTable, 'user, 'product, 'amount, 'rowtime.rowtime)
    tenv.createTemporaryView("goods_info", dataStream, 'promotion_id, 'sales_number,  'row_time.rowtime)

    var hop_parma="INTERVAL '1' MINUTE, INTERVAL '24' HOUR"
    val resultTable= tenv.sqlQuery("select  HOP_START(row_time,"+hop_parma+") start_time," +
      " HOP_END(row_time, "+hop_parma+") end_time," +
      "promotion_id,max(sales_number),min(sales_number) from goods_info  " +
      " GROUP BY HOP( row_time  ,"+hop_parma+"), promotion_id   ")
    val resultDataStream=tenv.toRetractStream[(Timestamp,Timestamp,String,Integer,Integer)](resultTable)
      .filter(x=>x._2._4!=x._2._5)




    resultDataStream.print()
    env.execute()
  }


}
