package com.other

import java.net.URI
import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import com.xiaohulu.bean.{AnchorResultBean, DataBean, FansInfoResultBean, FansListResultBean, GoodsInfoBean, GoodsResultBean}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import com.google.gson.{Gson, JsonParser}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.runtime.state.filesystem.FsStateBackend

import scala.collection.JavaConverters._
object KafkaHandler {

  private val KAFKA_BROKER = "Kafka-01:9092"
  //private val KAFKA_BROKER = "219.129.216.209:9092"
  private val TRANSACTION_GROUP = "com.wugenqiang.flink"

  def main(args : Array[String]){
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = new URI("file:///tmp/checkpoint")
    env.setStateBackend(new FsStateBackend(source));
    env.enableCheckpointing(1000*60)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //env.setStateBackend(new MemoryStateBackend(1000, false))
    //env.setStateBackend( new RocksDBStateBackend(new URI("hdfs:///192.168.120.160/check_point/")))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    // configure Kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    val transaction =
      env.addSource(
        new FlinkKafkaConsumer[String]("crawler-data-scene-goods-infos-71", new SimpleStringSchema(), kafkaProps)
      )

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
        goodsInfoList :+= g
      })
      goodsInfoList
    })

//      //filter(x=>x.item.size==0)
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[GoodsInfoBean](Time.seconds(60)) {
//        override def extractTimestamp(t: GoodsInfoBean): Long = t.time*1000
//      })
   .assignAscendingTimestamps(_.time*1000)
      .keyBy(_.promotion_id).timeWindow(Time.hours(48),Time.minutes(1))
      .aggregate(new AggregateTest)
      .filter(x=>x._3!=x._2)
        .map(x=>{
          var strTime :String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(x._4*1000)
          (x._1,x._2,x._3,strTime)
        })
    dataStream.print()
    env.execute()
  }

}
class AggregateTest extends AggregateFunction[GoodsInfoBean,(String ,Long,Long,Long),(String ,Long,Long,Long)]{
  override def createAccumulator(): (String, Long, Long,Long) = ("",0,0,0)

  override def add(in: GoodsInfoBean, acc: (String, Long, Long,Long)): (String, Long, Long,Long) = {
    var lowSalesNum= acc._2
    var highSalesNum=acc._3
    var time=acc._4
    if (time==0l){
      time=in.time
    }else{
      if (in.time<time){
        time=in.time
      }

    }
    if(in.sales_number<lowSalesNum){
      lowSalesNum=in.sales_number
    }
    if(in.sales_number>highSalesNum){
      highSalesNum=in.sales_number
    }
    if(lowSalesNum==0d){
      lowSalesNum=in.sales_number
    }

    (in.promotion_id,lowSalesNum,highSalesNum,time)

  }

  override def getResult(acc: (String, Long, Long,Long)): (String, Long, Long,Long) = (acc._1,acc._2,acc._3,acc._4)

  override def merge(acc: (String, Long, Long,Long), acc1: (String, Long, Long,Long)): (String, Long, Long,Long) = {
    var lowSalesNum= acc._2
    var highSalesNum=acc._3
    var time=acc._4
    if(acc1._2<lowSalesNum){
      lowSalesNum=acc1._2
    }
    if(acc1._3>highSalesNum){
      highSalesNum=acc1._3
    }
    if(acc1._4<time){
      time=acc1._4
    }

    (acc._1,lowSalesNum,highSalesNum,time)
  }
}
