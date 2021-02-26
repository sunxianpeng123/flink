package com.xiaohulu.demo.test.transform

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.google.gson.{Gson, JsonParser}
import com.xiaohulu.demo.test.bean.{GoodsDataBean, GoodsResM, GoodsResultBean, GoodsTransMid}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.time.Time.seconds
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
/**
  * @author xq
  * @date 2020/12/15 14:31
  * @version 1.0
  * @describe:
  */
object TransformGoods {

  /**
    * 第一步从kafka中得到数据转对象
    * @param input
    * @return
    */
  def sourceTrans(input:DataStream[String]):DataStream[Array[GoodsDataBean]]={
    val line = input.map(x=>x).map(line=>{
      val gs = new Gson()
      val je = JsonParser.parseString(line)
      var dbArr: Array[GoodsDataBean] = null
      if (je.isJsonArray) {
        dbArr = gs.fromJson(je, classOf[Array[GoodsDataBean]])
      } else {
        val dbb = gs.fromJson(je, classOf[GoodsDataBean])
        dbArr = new Array[GoodsDataBean](1)
        dbArr(0) = dbb
      }
      dbArr
    }).uid("s2")

    line
  }

  /**
    * 第二次加工
    * @param arrStream
    * @return
    */
  def dataSourceToArrObj(arrStream:DataStream[Array[GoodsDataBean]]):DataStream[Array[Array[GoodsResultBean]]]= {
    val objArr: DataStream[Array[Array[GoodsResultBean]]] =
      arrStream.filter(_ != null).uid("s3")
        .map(arr => {
          val arrM: Array[Array[GoodsResultBean]] = arr.map(line => {
            var goodsArr: Array[GoodsResultBean] = Array.empty
            val sdf = new SimpleDateFormat("yyyyMMdd")
            val gs = new Gson()
            val sid: String = line.sid
            val roomid: String = line.roomid
            val time: Long = line.time

            if (line.item != null & line.item.length > 0) {
              line.item.foreach(item=>{
                val itemTime = item.time
                val uid = item.uid
                if(item.goodsInfoList != null){
                  item.goodsInfoList.foreach(listbean=>{
                    val goodsResultBean = new GoodsResultBean
                    goodsResultBean.platform_id = sid

                    goodsResultBean.product_id = listbean.product_id
                    goodsResultBean.promotion_id = listbean.promotion_id
                    goodsResultBean.live_id = item.liveId
                    goodsResultBean.room_id = item.uid
                    goodsResultBean.current_promotion_id = item.current_promotion_id

                    goodsResultBean.cover = listbean.cover
                    goodsResultBean.is_virtual = listbean.is_virtual
                    goodsResultBean.in_stock = listbean.in_stock
                    goodsResultBean.stock_num = listbean.stock_num
                    goodsResultBean.cos_fee = listbean.cos_fee

                    goodsResultBean.cos_ratio = listbean.cos_ratio
                    goodsResultBean.platform = listbean.platform
                    goodsResultBean.sales_number = listbean.sales_number
                    goodsResultBean.price = listbean.price
                    goodsResultBean.min_price = listbean.min_price


                    goodsResultBean.platform_label = listbean.platform_label
                    goodsResultBean.item_type = listbean.item_type
                    goodsResultBean.index = listbean.index
                    //                    goodsResultBean.label_icon = listbean
                    goodsResultBean.shop_id = listbean.shop_id
                    goodsResultBean.detail_url = listbean.detail_url

                    goodsResultBean.title = listbean.title
                    goodsResultBean.short_title = listbean.short_title
                    goodsResultBean.images = gs.toJson(listbean.images.toList.asJava)
                    goodsResultBean.platform_sales_number = listbean.platform_sales_number
                    goodsResultBean.coupon = listbean.coupon

                    goodsResultBean.seckill_start_time = listbean.seckill_start_time
                    goodsResultBean.seckill_end_time = listbean.seckill_end_time
                    goodsResultBean.seckill_title = listbean.seckill_title
                    goodsResultBean.seckill_left_stock = listbean.seckill_left_stock
                    goodsResultBean.seckill_stock = listbean.stock_num

                    goodsResultBean.seckill_min_price = listbean.seckill_min_price
                    goodsResultBean.seckill_max_price = listbean.seckill_max_price
                    goodsResultBean.tb_categoryId = listbean.tb_categoryId
                    goodsResultBean.tb_rootCategoryId = listbean.tb_rootCategoryId
                    goodsResultBean.platform_price = listbean.platform_price

                    goodsResultBean.views = listbean.views
                    if (itemTime.equals("")) goodsResultBean.timestamp = time.toString
                    else goodsResultBean.timestamp = itemTime
                    val date = sdf.format(new Date(goodsResultBean.timestamp.toLong * 1000))
                    goodsResultBean.date = date

                    goodsArr:+= goodsResultBean
                  })
                }

              })
            }
            goodsArr
          })
          arrM
        }).uid("s4")
    objArr

  }


  /**
    * 第三次加工, 平铺对象
    * @param input
    * @return
    */
  def getOneGoods(oneTrans:DataStream[Array[Array[GoodsResultBean]]]):DataStream[GoodsResultBean]={
    val one = oneTrans.flatMap(arr=>{
      arr.iterator
    }).uid("s5")
      .flatMap(arr2=>{
        val goodsArr = arr2.iterator
        goodsArr
      }).uid("s6")
    one
  }


  /**
    * 一下的方法被废弃不用，这个是做测试用的，真实的用 TransformFieldGoods 类下的方法
    *
    * 第四步  获取每分钟商品销售的增量 根据 (a.promotionId,a.roomId,a.liveId) 聚合
    * @param goods
    * @return  如果用 流聚合的coGroup 的方式 前三个字段 要合并成一个元组
    *    注册Table方式返回：(room_id, live_id, time_minut, promotion_id, sales_number,min_price, date, incr_sales, start_time)
    *
    *    (room_id, live_id, promotion_id, sales_number,min_price, date, incr_sales, start_time)
    *
    */
  def getGoodsIncrFun(goods:DataStream[GoodsResultBean]): DataStream[GoodsResM] ={
    val resStream = goods.map(a=>{
      GoodsTransMid(a.promotion_id,a.room_id,a.live_id,a.sales_number,a.min_price,a.timestamp.toLong)
    }).uid("s7")
      //.assignTimestampsAndWatermarks(new MyPeriodicWatermark)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[GoodsTransMid](seconds(20)){
      override def extractTimestamp(element: GoodsTransMid): Long = element.timeStamp * 1000L  //毫秒
    }).uid("s8")
      .map(a=>{
        //((a.promotionId,a.roomId,a.liveId),a.salesNumber,a.minPrice,a.timeStamp,a.date,0,a.timeStamp) //最后两个为：增量，开始时间
        ((a.promotionId,a.roomId,a.liveId),a.salesNumber,a.minPrice,a.timeStamp)
      }).uid("s9")
      //.keyBy("promotionId","roomId","liveId")
      .keyBy(_._1)
      //.timeWindow(Time.days(7),Time.minutes(5))         //服务器
      .timeWindow(Time.minutes(10),Time.minutes(1))  //本地
      .allowedLateness(Time.minutes(3))
      .aggregate(new MyGoodsAggregateFunction,new WindowResultF).uid("s10")

    resStream
  }

}

/**
  * 商品信息增量计算
  * (商品id,主播id,场次id),销售量,最小价,时间戳,日期
  * ((String,String,String),Int,Double,Long,String)
  *
  * 商品id,主播id,场次id,销量,最小价,时间戳,新增，趋势
  * (String,String,String,Int,Double,Long,Int,String)
  */
class MyGoodsAggregateFunction extends AggregateFunction[((String,String,String),Int,Double,Long),
  (String,String,String,Int,Double,Long,Int,String),(String,String,String,Int,Double,Long,Int,String)] {
  var map = new util.HashMap[String, Int]()
  //var mapTrend = new util.HashMap[String, String]()

  override def createAccumulator(): (String, String, String, Int, Double, Long, Int,String) = {
    ("", "", "", 0, -1.0, 0L, 0,"")
  }

  override def add(value: ((String, String, String), Int, Double, Long), accumulator: (String, String, String, Int, Double, Long, Int,String)): (String, String, String, Int, Double, Long, Int,String) = {

    //销量
    var first = value._2
    val trend = accumulator._8
    var key = value._1._1 + "_" + value._1._2 + "_" + value._1._3
    if (map.get(key) == null || map.get(key) == 0) {
      map.put(key, accumulator._4)
      //mapTrend.put(key,accumulator._8)
    } else {
      first = map.get(key)
    }


    val promotionId = value._1._1
    val roomId = value._1._2
    val liveId = value._1._3
    var timestamp = accumulator._6
    var salesNumber = accumulator._4 //本窗口最大时间的销量
    var minPrice = accumulator._5 //最低价
    if (value._4.toLong > timestamp) {
      timestamp = value._4
      salesNumber = value._2
    }

    if (minPrice > value._3 || minPrice < 0) {
      minPrice = value._3
    }

    val incr = salesNumber - first

    (promotionId, roomId, liveId, salesNumber, minPrice, timestamp, incr,trend)
  }

  override def getResult(accumulator: (String, String, String, Int, Double, Long, Int,String)): (String, String, String, Int, Double, Long, Int,String) = {
    //val trend  = accumulator._8
    val time = accumulator._6
    val sale_num = accumulator._4
    //var trend_des = ""
    val sb = new StringBuilder
    sb.append("[{\"time\":")
    sb.append(time)
    sb.append(",\"sn\":")
    sb.append(sale_num)
    sb.append("}]")
    val trend_des = sb.toString()
    map.clear()
    //mapTrend.clear()
    (accumulator._1, accumulator._2, accumulator._3, sale_num , accumulator._5, time , accumulator._7, "")
  }

  override def merge(a: (String, String, String, Int, Double, Long, Int,String), b: (String, String, String, Int, Double, Long, Int,String)): (String, String, String, Int, Double, Long, Int,String) = {
    val promotionId = a._1
    val roomId = a._2
    val liveId = a._3
    var timestamp = a._6
    var salesNumber = a._4
    var minPrice = a._5

    if (b._6.toLong > timestamp) {
      timestamp = b._6
      salesNumber = b._4
    }
    if (b._5 <= minPrice) { //全窗口最低价
      minPrice = b._5
    }
    //var trend = a._8
    var incr = a._7
    if(a._7 ==null || a._7 ==0){
      //trend=b._8
      incr=b._7
    }

    (promotionId, roomId, liveId, salesNumber, minPrice, timestamp, incr, "")

  }

}

//自定义排序输出处理函数
class WindowResultF() extends ProcessWindowFunction[(String, String, String, Int, Double, Long, Int,String),GoodsResM,(String,String,String),TimeWindow]{
  val ttlConfig  = StateTtlConfig
    .newBuilder(org.apache.flink.api.common.time.Time.seconds(60 * 10)) //设置过期时间
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .cleanupInRocksdbCompactFilter(1000)
    .build()

  //趋势
  var trendState: MapState[String, String] = _
  var trendStateDesc: MapStateDescriptor[String, String] = _
  //场次第一次销量（为了计算整场次的销量）
  var firstSaleState: MapState[String,Int] = _
  var firstSaleStateDesc: MapStateDescriptor[String,Int] = _

  override def open(parameters: Configuration): Unit = {
    //super.open(parameters)
    trendStateDesc = new MapStateDescriptor[String, String]("trendState", classOf[String], classOf[String])
    //设置周期
    trendStateDesc.enableTimeToLive(ttlConfig)
    trendState = getRuntimeContext.getMapState(trendStateDesc)

    firstSaleStateDesc = new MapStateDescriptor[String, Int]("firstSaleState", classOf[String],classOf[Int])
    firstSaleStateDesc.enableTimeToLive(ttlConfig)
    firstSaleState = getRuntimeContext.getMapState(firstSaleStateDesc)
  }

  override def process(key: (String, String, String), context: Context, elements: Iterable[(String, String, String, Int, Double, Long, Int, String)], out: Collector[GoodsResM]): Unit = {
    val endWindowTime = context.window.getEnd/1000

    val last = elements.iterator.next()
    val key = last._2 + "_" + last._1 + "_" + last._3
    val trend = trendState.get(key)
    //val trend  = last._8
    //timestamp时间
    val time = last._6
    val sale_num = last._4
    var trend_des = ""
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
      firstSaleState.put(key,sale_num)
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

      //本场次增加
      val firstState = firstSaleState.get(key)
      live_inc = sale_num - firstState
    }

    out.collect(GoodsResM(last._2, last._1, last._3, last._5, last._4, last._7,trend_des,live_inc))
  }
}


