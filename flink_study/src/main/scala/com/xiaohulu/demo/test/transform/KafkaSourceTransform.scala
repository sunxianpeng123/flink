package com.xiaohulu.demo.test.transform

import java.text.SimpleDateFormat
import java.util.Date

import com.google.gson.{Gson, JsonParser}
import com.xiaohulu.demo.test.bean._
import com.xiaohulu.demo.test.utils.DateUtil
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.JavaConverters._
/**
  * @author xq
  * @date 2020/8/6 10:44
  * @version 1.0
  * @describe:
  */
object KafkaSourceTransform {
  /**
    * 第一步从kafka中得到数据转对象
    * @param input
    * @return
    */
  def sourceTrans(input:DataStream[String]):DataStream[Array[DataBean]]={
    val line = input.map(x=>x).map(line=>{
      val gs = new Gson()
      val je = JsonParser.parseString(line)
      var dbArr: Array[DataBean] = null
      if (je.isJsonArray) {
        dbArr = gs.fromJson(je, classOf[Array[DataBean]])
      } else {
        val dbb = gs.fromJson(je, classOf[DataBean])
        dbArr = new Array[DataBean](1)
        dbArr(0) = dbb
      }
      dbArr
    }).uid("s2")

    line
  }

  /**
    * 第二步，将 Array[DataBean] 转换成更多的包装对象
    * @param arrStream
    */
  def dataSourceToArrObj(arrStream:DataStream[Array[DataBean]]):DataStream[Array[(Array[AnchorResultBean],Array[GoodsResultBean])]]={
    val objArr:DataStream[Array[(Array[AnchorResultBean],Array[GoodsResultBean])]] = arrStream.filter(_!=null).uid("s3")
      .map(arr=>{
        val arrM: scala.Array[(Array[AnchorResultBean],Array[GoodsResultBean])] = arr.map(bean => {
          var type_name = ""
          var platID = ""
          var time = ""
          var itemTime = ""

          val anchorBasicMark = "scene_basic"
          val goodsMark = "scene_goods_infos"

          val sdf = new SimpleDateFormat("yyyyMMdd")
          val gs = new Gson()

          var anchorBasicInfoArr: Array[AnchorResultBean] = Array.empty
          var goodsInfoArr: Array[GoodsResultBean] = Array.empty

          if (bean != null & bean.item != null & bean.item.length > 0) {
            type_name = bean.type_name
            platID = bean.sid
            time = bean.time
            //roomID = bean.roomid
            var anchorBasicBean: AnchorResultBean = null
            var goodsResultBean: GoodsResultBean = null

            bean.item.foreach(item => {
              itemTime = item.time
              anchorBasicBean = new AnchorResultBean
              if (item != null) {
                //todo 抖音数据
                if (item.typeName.equals(anchorBasicMark)) {
                  //主播基础信息数据
                  anchorBasicBean.platformId = platID
                  anchorBasicBean.room_id = item.uid
                  anchorBasicBean.liveId = item.liveId
                  anchorBasicBean.nickname = item.nickname
                  anchorBasicBean.display_id = item.display_id

                  anchorBasicBean.secId = item.secId
                  anchorBasicBean.secret = item.secret
                  anchorBasicBean.head = item.head
                  anchorBasicBean.gender = item.gender
                  anchorBasicBean.introduce = item.introduce

                  anchorBasicBean.level = item.level
                  anchorBasicBean.totalViewer = item.totalViewer
                  anchorBasicBean.onlineViewer = item.onlineViewer
                  anchorBasicBean.dySceneValue = item.dySceneValue
                  anchorBasicBean.dyValue = item.dyValue

                  anchorBasicBean.dyCoinOut = item.dyCoinOut
                  anchorBasicBean.fansCount = item.fansCount
                  anchorBasicBean.followCount = item.followCount
                  anchorBasicBean.location = item.location
                  anchorBasicBean.title = item.title

                  anchorBasicBean.cover = item.cover
                  if (itemTime.equals("")) anchorBasicBean.timestamp = time
                  else anchorBasicBean.timestamp = itemTime
                  val date = sdf.format(new Date(anchorBasicBean.timestamp.toLong * 1000))
                  anchorBasicBean.date = date

                  anchorBasicInfoArr :+= anchorBasicBean

                }

                if (item.typeName.equals(goodsMark)) {
                  //抖音带货数据
                  item.goodsInfoList.foreach(listbean => {
                    goodsResultBean = new GoodsResultBean
                    goodsResultBean.platform_id = platID

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
                    if (itemTime.equals("")) goodsResultBean.timestamp = time
                    else goodsResultBean.timestamp = itemTime
                    val date = sdf.format(new Date(goodsResultBean.timestamp.toLong * 1000))
                    goodsResultBean.date = date

                    goodsInfoArr :+= goodsResultBean
                  })
                }


              }
            })
          }
          (anchorBasicInfoArr,goodsInfoArr)
        })

        arrM
      }).uid("s4")

    objArr

  }

  /**
    * 第三步，平铺对象 获取 主播基础信息
    * @param packageObj
    * @return
    */
  def tObjGetOneAnchor(packageObj:DataStream[Array[(Array[AnchorResultBean],Array[GoodsResultBean])]]):DataStream[AnchorResultBean]={

    val one = packageObj.flatMap(arr=>{
      arr.iterator
    }).uid("s5").flatMap(arr2=>{
      val anchorArr = arr2._1
      val res = if(anchorArr!=null && anchorArr.length>0){
        anchorArr.iterator
      }else{
        val it = List.empty
        it.iterator
      }
      res
    }).uid("s6")

    one

  }

  /**
    * 第四步, 平铺对象 获取商品数据转对象
    * @param input
    * @return
    */
  def tObjGetOneGoods(packageObj:DataStream[Array[(Array[AnchorResultBean],Array[GoodsResultBean])]]):DataStream[GoodsResultBean]={
    val one = packageObj.flatMap(arr=>{
      arr.iterator
    }).uid("s8").flatMap(arr2=>{
      val goodsArr = arr2._2
      val res = if(goodsArr!=null && goodsArr.length>0){
        goodsArr.iterator
      }else{
        val it = List.empty
        it.iterator
      }
      res
    }).uid("s9")
    one
  }

  /**
    * 第五步  获取每分钟商品销售的增量 根据 (a.promotionId,a.roomId,a.liveId) 聚合
    * @param goods
    * @return  如果用 流聚合的coGroup 的方式 前三个字段 要合并成一个元组
    *    注册Table方式返回：(room_id, live_id, time_minut, promotion_id, sales_number,min_price, date, incr_sales, start_time)
    *
    */
  def getGoodsIncrInfo(goods:DataStream[GoodsResultBean]): DataStream[(String,String,String,String,Int,Double,String,Int,Long)] ={
    val resStream = goods.map(a=>{
      GoodsTrans(a.promotion_id,a.room_id,a.live_id,a.sales_number,a.min_price,a.timestamp.toLong,a.date)
    }).uid("s11")
      //.assignTimestampsAndWatermarks(new MyPeriodicWatermark)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[GoodsTrans](Time.seconds(180)){
      override def extractTimestamp(element: GoodsTrans): Long = element.timeStamp * 1000L  //毫秒
    }).uid("s12")
      .map(a=>{
        //((a.promotionId,a.roomId,a.liveId),a.salesNumber,a.minPrice,a.timeStamp,a.date,0,a.timeStamp) //最后两个为：增量，开始时间
        ((a.promotionId,a.roomId,a.liveId),a.salesNumber,a.minPrice,a.timeStamp,a.date)
      }).uid("s13")
      //.keyBy("promotionId","roomId","liveId")
      .keyBy(_._1)
      //.timeWindow(Time.days(2),Time.minutes(1))         //服务器
      .timeWindow(Time.minutes(10),Time.minutes(1))  //本地
      .aggregate(new GoodsAggregateFunction).uid("s14")
      .map(x=>{
        val timestamp = x._6 * 1000L
        val strDateTime = DateUtil.timestampToDateStr(timestamp)
        val timeMinut = strDateTime.substring(0,12)
        (x._2,x._3,timeMinut,x._1,x._4,x._5,x._7,x._8,x._6)
      }).uid("s15")

    resStream
  }


  /**
    * 第六步  获取每分钟主播信息 根据 (a.roomId,a.liveId) 聚合
    * @param anchor
    * @return 如果用 流聚合的coGroup 的方式 前三个字段 要合并成一个元组
    *    注册Table方式返回：(room_id, live_id, time_minut, totalviewer, onlineviewer,timestamp, date)
    */
  def getAnchorInfo(anchor:DataStream[AnchorResultBean]):DataStream[(String,String,String,Int,Int,Long,String)] ={
    val resStreamAnchor = anchor.map(a=>{
      AnchorTrans(a.room_id,a.liveId,a.totalViewer,a.onlineViewer,a.timestamp.toLong,a.date)
    }).uid("s16")
      //.assignTimestampsAndWatermarks(new MyPeriodicWatermark)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[AnchorTrans](Time.seconds(180)){
      override def extractTimestamp(element: AnchorTrans): Long = element.timeStamp * 1000L  //毫秒
    }).uid("s17")
      .map(a=>{
        //((a.roomId,a.liveId),a.totalViewer,a.onlineViewer,a.timeStamp,a.date,0,0)
        ((a.roomId,a.liveId),a.totalViewer,a.onlineViewer,a.timeStamp,a.date)
      }).uid("s18")
      .keyBy(_._1)
      //.timeWindow(Time.days(2),Time.minutes(1))         //服务器
      .timeWindow(Time.minutes(10),Time.minutes(1)) //本地
      .aggregate(new AnchorAggregateFunction).uid("s19")
      .map(x=>{
        val timestamp = x._5 * 1000L
        val strDateTime = DateUtil.timestampToDateStr(timestamp)
        val timeMinut = strDateTime.substring(0,12)
        (x._1,x._2,timeMinut,x._3,x._4,x._5,x._6)
      }).uid("s20")


    resStreamAnchor
  }
}
