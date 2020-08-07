package com.xiaohulu


import com.xiaohulu.conf.{ConfigTool, ParameterTool}
import com.xiaohulu.transform.extractor.{DyAnchorExtractor, DyGoodsExtractor}
import com.xiaohulu.transform.FlinkStreamMap
import com.xiaohulu.transform.windowFunction.{GoodsPromotionAggregate, GoodsSalesNumAggregate}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.types.Row


import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.streaming.api.scala.extensions._

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/23
  * \* Time: 18:27
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object FlinkScheduleApp_Stream {
  def main(args: Array[String]): Unit = {
    val (env, tEnv) = ParameterTool.getFLinkExecutionEnvironment()
    /** data source */
    //    consumer
    val dyAnchorConsumer = new FlinkKafkaConsumer[String](ConfigTool.anchor_basic_info_topics, new SimpleStringSchema(), ConfigTool.kafkaProps)
    val dyGoodsConsumer = new FlinkKafkaConsumer[String](ConfigTool.anchor_goods_topics, new SimpleStringSchema(), ConfigTool.kafkaProps)
    //    add source
    val dyAnchorSourceDataStream = env.addSource(dyAnchorConsumer)
    val dyGoodsSourceDataStream = env.addSource(dyGoodsConsumer)
    println("start data process")
    /** data transform */
    val dyAnchorDataStream = FlinkStreamMap.analysisDyAnchorKafkaStream(dyAnchorSourceDataStream)
      .assignTimestampsAndWatermarks(new DyAnchorExtractor)

    val dyGoodsDataStream = FlinkStreamMap.analysisDyGoodsKafkaStream(dyGoodsSourceDataStream)
      .assignTimestampsAndWatermarks(new DyGoodsExtractor)
    /** adapter */
    //todo 抖音Goods相关的计算
    println("计算Goods相关数据=============")
    val dyGoodsWindowStream = dyGoodsDataStream
      .keyBy(e => (e.platform_id, e.room_id, e.live_id, e.promotion_id))
      .timeWindow(Time.minutes(10), Time.minutes(1)) //滑动窗口
      //.timeWindow(Time.minutes(10))//滚动窗口
      .aggregate(new GoodsSalesNumAggregate)

    //dyGoodsWindowStream.print()
    val dyGoodsPromotionWindowStream = dyGoodsDataStream
      .keyBy(_.promotion_id)
      .timeWindow(Time.minutes(10), Time.minutes(1))
      .aggregate(new GoodsPromotionAggregate)

    //tEnv.createTemporaryView("goods_sales_number_tb", dyGoodsWindowStream, 'platform_id,'room_id, 'promotion_id, 'live_id, 'timestamp, 'sales_number)
    val goods_sales_number_tb = "goods_sales_number_tb"
    val goods_promotion_agg_tb = "goods_promotion_agg_tb"
    tEnv.createTemporaryView(goods_sales_number_tb, dyGoodsWindowStream)
    tEnv.createTemporaryView(goods_promotion_agg_tb, dyGoodsWindowStream)
    val joinSql =
      s"""
        |select * from $goods_sales_number_tb g
        | left outer join $goods_promotion_agg_tb a
        | on g.platform_id = a.platform_id and g.promotion_id=a.promotion_id
      """.stripMargin
    /** 打印查询表的几种方法 */
    val r = tEnv.sqlQuery(joinSql)
    r.printSchema()
    r.toRetractStream[Row].print()
//    val r2 = tEnv.executeSql(joinSql)
//    r2.print()

    //  dyGoodsPromotionWindowStream.print()
    //    val dyGoodsWindowJoinStream = dyGoodsWindowStream.join(dyGoodsPromotionWindowStream).where(x=>(x.platform_id,x.promotion_id)).equalTo(y=>(y.platform_id,y.promotion_id))
    //      .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
    //      .apply(new JoinFunction[GoodsSaleNumBean,GoodsPromotionBean,(String,String,Int)] {
    //        override def join(in1: GoodsSaleNumBean, in2: GoodsPromotionBean) = {
    //          (in1.platform_id,in1.promotion_id,)
    //        }
    //      })

    //    val dyAnchorWindowStream = dyAnchorDataStream.map(e => (e.platformId, e.room_id, e.timestamp.toLong / 300 * 300.toLong, e.onlineViewer, e.totalViewer))
    //      .keyBy(e => (e._1, e._2, e._3)).timeWindow(Time.seconds(2), Time.seconds(1)).aggregate(new AnchorViewerAggregate)
    //    dyAnchorWindowStream.print()

    /** data sink */
    //sink config
    //    val dyAnchorStreamingFileSink = StreamingFileSink.forBulkFormat(new Path(ConfigTool.anchor_basic_path), ParquetAvroWriters.forReflectRecord(classOf[AnchorResultBean])).withBucketAssigner(new DateTimeBucketAssigner[AnchorResultBean]("yyyyMMdd")).build()
    //    val dyGoodsStreamingFileSink = StreamingFileSink.forBulkFormat(new Path(ConfigTool.goods_info_path), ParquetAvroWriters.forReflectRecord(classOf[GoodsResultBean])).withBucketAssigner(new DateTimeBucketAssigner[GoodsResultBean]("yyyyMMdd")).build()

    //    add sink
    //    anchorRs.toAppendStream[AnchorResultBean].print()//批转流
    println("down")
    env.execute("Flink Anchor Stream Live Schedule")


  }
}

