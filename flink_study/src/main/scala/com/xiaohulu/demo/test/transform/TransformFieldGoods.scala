package com.xiaohulu.demo.test.transform

import java.util

import com.xiaohulu.demo.test.bean.{GoodsResultBean, GoodsTransMid2, TransAcc, TransAccRes}
import com.xiaohulu.demo.test.utils.DateUtil
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.time.Time.seconds
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @author xq
  * @date 2020/12/17 18:33
  * @version 1.0
  * @describe:
  */
object TransformFieldGoods {

  def getFieldGoodsIncrFun(goods:DataStream[GoodsResultBean],windowSize:Int,slidingSize:Int,sleepSize:Int): DataStream[TransAccRes] ={
    val resStream = goods.map(a=>{
      GoodsTransMid2(a.platform_id,a.room_id,a.promotion_id,a.live_id,a.product_id,a.in_stock,a.stock_num,a.cos_fee,
        a.cos_ratio,a.platform,a.sales_number,a.cover,a.is_virtual,a.price,a.min_price,a.platform_label,a.item_type,
        a.shop_id,a.detail_url,a.title,a.short_title,a.timestamp.toLong)
    }).uid("s7")
      //.assignTimestampsAndWatermarks(new MyPeriodicWatermark).uid("s8")
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[GoodsTransMid2](seconds(sleepSize)){
      override def extractTimestamp(element: GoodsTransMid2): Long = element.timeStamp * 1000L  //毫秒
    }).uid("s8")
      .map(a=>{
        ((a.platformId,a.roomId,a.promotionId,a.liveId),a.productId,a.inStock,
          a.stockNum,a.cosFee,a.cosRatio,a.platform,a.salesNumber,a.cover,a.isVirtual,
          a.price,a.minPrice,a.platformLabel,a.itemType,a.shopId,a.detailUrl,a.title,
          a.shortTitle:String,a.timeStamp)
      }).uid("s9")
      //.keyBy("promotionId","roomId","liveId")
      .keyBy(_._1)
      .timeWindow(Time.hours(windowSize),Time.minutes(slidingSize))         //服务器
      //.timeWindow(Time.days(windowSize),Time.minutes(slidingSize))  //本地
      //.allowedLateness(Time.minutes(3))
      .aggregate(new MyFieldGoodsAggregateFunction,new MyFieldWindowResultF(windowSize)).uid("s10")

    resStream
  }

}

class MyPeriodicWatermark extends AssignerWithPeriodicWatermarks[GoodsTransMid2]{
  //1分钟 ，单位毫秒
  private val maxOutofOrderness:Long = 60000L
  //var currendMaxTimestamp:Long = Long.MinValue
  var currendMaxTimestamp:Long = _

  override def extractTimestamp(element: GoodsTransMid2, recordTimestamp: Long): Long = {
    val timestamp = element.timeStamp
    currendMaxTimestamp = Math.max(timestamp, currendMaxTimestamp)
    println("=============== timestamp ====================== " + currendMaxTimestamp )
    currendMaxTimestamp
  }

  override def getCurrentWatermark: Watermark = {
    val watermark = currendMaxTimestamp*1000L - maxOutofOrderness
    println("=============== Watermark ====================== " + watermark )
    new Watermark(watermark)
  }
}

/**
  * 做聚合累计操作
  */
class MyFieldGoodsAggregateFunction extends AggregateFunction[((String,String,String,String),String,Boolean,
  Int,Double,Double,Int,Int,String,String,Double,Double,String,String,String,String,String,String,Long),
  TransAcc, TransAcc] {
  var map = new util.HashMap[String, Int]()

  override def createAccumulator(): TransAcc = {
    TransAcc("","","","","",false,0,0,0,0,0,"","",0,0,"","","","","","",0,0,"")
  }

  override def add(value: ((String, String, String, String), String, Boolean, Int, Double, Double, Int, Int, String, String, Double, Double, String, String, String, String, String, String, Long), accumulator: TransAcc): TransAcc = {
    //为了保存获取滑动窗口上次末起始销量
    var first = value._8
    val key = value._1._1 + "_" + value._1._2 + "_" + value._1._3+ "_" + value._1._4
    if (map.get(key) == null || map.get(key) == 0) {
      map.put(key, accumulator.salesNumber)
    } else {
      first = map.get(key)
    }

    var timestamp = accumulator.timeStamp
    var salesNumber = accumulator.salesNumber //本窗口最大时间的销量
    var productId = accumulator.productId
    var inStock = accumulator.inStock
    var stockNum = accumulator.stockNum
    var cosFee = accumulator.cosFee
    var cosRatio = accumulator.cosRatio
    var platform = accumulator.platform

    var cover = accumulator.cover
    var isVirtual = accumulator.isVirtual
    var price = accumulator.price

    var platformLabel = accumulator.platformLabel
    var itemType = accumulator.itemType
    var shopId = accumulator.shopId
    var detailUrl = accumulator.detailUrl
    var title = accumulator.title
    var shortTitle = accumulator.shortTitle

    if (value._19 > timestamp) {
      //下面赋值，都是最新时间值
      timestamp = value._19
      salesNumber = value._8
      productId = value._2
      inStock = value._3
      stockNum = value._4
      cosFee = value._5
      cosRatio = value._6
      platform = value._7

      cover = value._9
      isVirtual = value._10
      price = value._11
      platformLabel = value._13
      itemType = value._14
      shopId = value._15
      detailUrl = value._16
      title = value._17
      shortTitle = value._18

    }

    var minPrice = accumulator.minPrice //最低价
    if (minPrice > value._12 || minPrice <= 0) {
      minPrice = value._12
    }

    val incr = salesNumber - first

    val endDate = DateUtil.timeToString(timestamp * 1000,"yyyy-MM-dd")

    TransAcc(value._1._1,value._1._2,value._1._3,value._1._4,productId, inStock, stockNum, cosFee, cosRatio, platform, salesNumber,
      cover, isVirtual, price, minPrice, platformLabel, itemType, shopId, detailUrl, title, shortTitle, timestamp, incr, endDate)

  }

  override def getResult(accumulator: TransAcc): TransAcc = {
    accumulator
  }


  override def merge(a: TransAcc, b: TransAcc): TransAcc = {
    if(a.timeStamp < b.timeStamp){
      b
    }else{
      a
    }
  }
}


class MyFieldWindowResultF(windowSize:Int)  extends ProcessWindowFunction[TransAcc,TransAccRes,(String, String, String, String),TimeWindow]{
  val ttlConfig  = StateTtlConfig
    //.newBuilder(org.apache.flink.api.common.time.Time.seconds(604800)) //设置过期时间 604800 :7天
    //.newBuilder(org.apache.flink.api.common.time.Time.days(windowSize)) //设置过期时间 604800 :7天
    .newBuilder(org.apache.flink.api.common.time.Time.hours(windowSize))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    //.cleanupInRocksdbCompactFilter(1000)
    .cleanupIncrementally(1000,true) //cleanupSize指定每次触发清理时检查的状态条数;true可以附加在每次写入/删除时清理
    .build()

  //趋势
  var trendState: MapState[String, String] = _
  var trendStateDesc: MapStateDescriptor[String, String] = _
  //场次第一次销量（为了计算整场次的销量）
  var firstSaleState: MapState[String,String] = _
  var firstSaleStateDesc: MapStateDescriptor[String,String] = _

  override def open(parameters: Configuration): Unit = {
    trendStateDesc = new MapStateDescriptor[String, String]("trendState", classOf[String], classOf[String])
    //设置周期
    trendStateDesc.enableTimeToLive(ttlConfig)
    trendState = getRuntimeContext.getMapState(trendStateDesc)

    firstSaleStateDesc = new MapStateDescriptor[String, String]("firstSaleState", classOf[String],classOf[String])
    firstSaleStateDesc.enableTimeToLive(ttlConfig)
    firstSaleState = getRuntimeContext.getMapState(firstSaleStateDesc)
  }

  override def process(keyt: (String, String, String, String), context: Context, elements: Iterable[TransAcc], out: Collector[TransAccRes]): Unit = {
    //获得窗口结束时间 秒
    val endWindowTime = context.window.getEnd/1000
    val startWindowTime = context.window.getStart/1000

    println("startWindowTime == "+startWindowTime + " &&  endWindowTime == " + endWindowTime)

    val last = elements.iterator.next()
    val key = keyt._1 + "_" + keyt._2 + "_" + keyt._3 + "_" + keyt._4
    val trend = trendState.get(key)
    //timestamp时间
    val time = last.timeStamp
    val sale_num = last.salesNumber
    var trend_des = ""
    var startDate = ""
    var onSaleTime = ""

    val endDate = DateUtil.timeToString(time * 1000,"yyyy-MM-dd")
    //本场次增加
    var live_inc = 0
    val sb = new StringBuilder
    if(trend == null || trend.length<1){
      sb.append("[{\"time\":")
      sb.append(endWindowTime)
      sb.append(",\"sn\":")
      sb.append(sale_num)
      sb.append("}]")
      trend_des = sb.toString()
      trendState.put(key,trend_des)

      startDate = DateUtil.timeToString(time * 1000,"yyyy-MM-dd")
      onSaleTime = DateUtil.timeToString(time * 1000,"yyyy-MM-dd HH:mm:ss")
      firstSaleState.put(key,sale_num + "&" + startDate + "&" + onSaleTime)
    }else{
      val prefix = trend.substring(0,trend.length-1)
      sb.append(prefix)
      sb.append(",{\"time\":")
      sb.append(endWindowTime)
      sb.append(",\"sn\":")
      sb.append(sale_num)
      sb.append("}]")
      trend_des = sb.toString()
      trendState.put(key,trend_des)

      val value = firstSaleState.get(key)
      val arr = value.split("&")
      live_inc = sale_num - arr(0).toInt

      startDate = arr(1)
      onSaleTime = arr(2)
      println("********************** xuqian ****************************")
    }
    out.collect(TransAccRes(last.platformId.toInt,last.roomId,last.promotionId,last.liveId,last.productId,last.inStock,last.stockNum,
      last.cosFee,last.cosRatio,last.platform,last.salesNumber,last.cover,last.isVirtual,last.price,last.minPrice,last.platformLabel,
      last.itemType,last.shopId,last.detailUrl,last.title,last.shortTitle,live_inc,trend_des,startDate,endDate,onSaleTime))

  }
}
