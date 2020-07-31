////package com.xiaohulu.adapter
////
////import com.xiaohulu.conf.ConfigTool
////
////import scala.collection.mutable.ArrayBuffer
////
/////**
////  * \* Created with IntelliJ IDEA.
////  * \* User: sunxianpeng
////  * \* Date: 2020/4/8
////  * \* Time: 13:55
////  * \* To change this template use File | Settings | File Templates.
////  * \* Description:
////  * \*/
////object DataFrameAdapter {
////
////  def transGoodInfoDF(anchorBasicInfoDF: DataFrame, goodsInfoDF: DataFrame, fixDF: DataFrame, promotionDF: DataFrame, date: String, sqlContext: SparkSession): DataFrame = {
////    val maxOnlineView_percent = ConfigTool.maxOnlineView_percent
////    val sn_add_percent = ConfigTool.sn_add_num
////    val sn_add_percent_10W = ConfigTool.sn_add_num / 15 //4
////    val sn_add_percent_1W = ConfigTool.sn_add_num / 10 //6
////    val sn_add_percent_5000 = ConfigTool.sn_add_num / 5 //12
////
////    //用户自定义函数
////    sqlContext.udf.register("regexp_extract_all", (promote_remark_max: String) => {
////      var arr: Array[String] = Array.empty
////      val pattern = "[\\d]+\\.*[\\d]*".r
////      val group = pattern.findAllIn(promote_remark_max)
////      while (group.hasNext) arr :+= group.next()
////      if (arr.length < 2) arr = Array("0", "0")
////      arr
////    }) //注册函数regexp_extract_all
////    //    val anchorDF:DataFrame = anchorBasicInfoDF.where(s"date = ${ date.replaceAll("-","") }")
////    //    val goodsDF:DataFrame = goodsInfoDF.where(s"date = ${ date.replaceAll("-","")}")
//    val anchor_basic_tb_name = "anchor_basic_tb_name"
//    val goods_tb_name = "good_info_tb"
////    anchorBasicInfoDF.createOrReplaceTempView(anchor_basic_tb_name)
////    goodsInfoDF.createOrReplaceTempView(goods_tb_name)
////
//    val sql_all =
//    s"""  select d.*, maxOnlineView,uv  from(select promotion_id, room_id ,live_id, floor(timestamp/300)*300 as time_t ,sum(sn_add) as sn_add , min(mp)  as min_price  from (
//          select promotion_id, live_id, room_id, timestamp  , lead(sn,1,sn) over (PARTITION BY promotion_id,room_id, live_id order by promotion_id, room_id, live_id,timestamp) -sn as sn_add   ,sn ,mp from(
//          select  timestamp ,room_id, t.promotion_id ,live_id, salesNum as sn  ,mp   from (
//          SELECT timestamp ,room_id, promotion_id,live_id, max(sales_number) as salesNum  FROM $goods_tb_name v  group by  promotion_id , room_id, live_id, timestamp) t
//            inner join (select promotion_id,if(mp >max_coupon,mp -max_coupon ,mp ) as mp from (
//            select promotion_id,max_coupon,if(mp >= standard,mp-discounts,mp) as mp from (
//            select promotion_id,mp,max_coupon,if(discounts_price is not null,cast(discounts_price[0] as double)*100.0,0.0) as standard,if(discounts_price is not null,cast(discounts_price[1] as double)*100.0,0.0) as discounts from(
//            select promotion_id,mp,max_coupon,if(promote_remark_max like '满%元,省%元',regexp_extract_all(promote_remark_max),null) as discounts_price from (
//            select  promotion_id,IF(max(seckill_min_price)>0, max(seckill_min_price), min(min_price)) as mp,max(coupon) as max_coupon  , max(promote_remark) as promote_remark_max FROM  $goods_tb_name v  group by  promotion_id))))
//            )m on t.promotion_id= m.promotion_id where mp > 0
//          ) b  order by promotion_id, room_id , live_id,timestamp) v
//          where sn_add != 0
//            group by  promotion_id, room_id ,live_id,floor(timestamp/300)*300  order by      room_id,    live_id, floor(timestamp/300)*300 ) d
//            left join (select room_id ,floor(timestamp/300)*300 as time_t , max(onlineviewer) as maxOnlineView, max(totalviewer) as uv from $anchor_basic_tb_name
//            group by room_id ,floor(timestamp/300)*300 ) a  on a.room_id=d.room_id  and a.time_t=d.time_t where sn_add > 0  order by      room_id,   maxOnlineView desc ,  live_id, time_t"""
//
//
////    println(sql_all)
//
////    val sql_vip =
////      s"""select d.*, maxOnlineView,uv  from(select promotion_id, room_id ,live_id, floor(timestamp/300)*300 as time_t ,sum(sn_add) as sn_add , min(mp)  as min_price, min(max_sales) from (
////                    select promotion_id, live_id, room_id, timestamp  , lead(sn,1,sn) over (PARTITION BY promotion_id,room_id, live_id  order by promotion_id, room_id, live_id,timestamp) -sn as sn_add   ,sn ,mp  ,max_sales from(
////                    select  timestamp ,room_id, t.promotion_id ,live_id, platform_sales_num as sn  ,mp ,max_sales from (
////                    SELECT timestamp ,room_id, promotion_id,live_id, max(sales_number) as salesNum  ,max(platform_sales_num)  as platform_sales_num  FROM $goods_tb_name v
////                     WHERE   platform_sales_num>0  and floor(min_price/100) not in (9999999,999999,99999)   group by  promotion_id , room_id, live_id, timestamp) t
////                    inner join (select promotion_id,max_sales,if(mp >max_coupon,mp -max_coupon ,mp ) as mp from (
////                    select promotion_id,max_coupon,max_sales,standard,discounts,if(mp >= standard,mp-discounts,mp) as mp from (
////                    select promotion_id,mp,max_coupon,max_sales,if(discounts_price is not null,cast(discounts_price[0] as double)*100.0,0.0) as standard,if(discounts_price is not null,cast(discounts_price[1] as double)*100.0,0.0) as discounts from (
////                    select promotion_id,mp,max_coupon,max_sales,if(promote_remark_max like '满%元,省%元',regexp_extract_all(promote_remark_max),null) as discounts_price from (
////                    select  promotion_id,IF(max(seckill_min_price)>0, max(seckill_min_price),  IF(min(platform_price)>0,min(platform_price) ,min(min_price) ))  as mp,max(coupon) as max_coupon  , max(promote_remark) as promote_remark_max,max(sales_number)-min(sales_number) as  max_sales FROM $goods_tb_name v WHERE   platform_sales_num>0  group by  promotion_id))))
////                    )m on t.promotion_id= m.promotion_id  where mp > 0 ) b
////                    order by promotion_id, room_id , live_id,timestamp) v where sn_add != 0
////                    group by  promotion_id, room_id ,live_id,floor(timestamp/300)*300  order by  room_id, live_id, floor(timestamp/300)*300 ) d
////                    left join (select room_id ,floor(timestamp/300)*300 as time_t , max(onlineviewer) as maxOnlineView, max(totalviewer) as uv
////                    from $anchor_basic_tb_name   group by room_id ,floor(timestamp/300)*300 ) a
////                    on a.room_id=d.room_id  and a.time_t=d.time_t  where sn_add >0  order by   promotion_id, room_id,   time_t   """
////    var allDF: DataFrame = null
////    var vipDF: DataFrame = null
////    fixDF.printSchema()
////
////    if (ConfigTool.where_to_run.equals("local")) {
////      val (allDF_return, vipDF_return) = CreateDataFrame.createGoodInfotransedDF(sqlContext)
////      allDF = allDF_return
////      vipDF = vipDF_return
////
////    } else {
////      allDF = sqlContext.sql(sql_all).selectExpr("promotion_id", "room_id", "live_id", "time_t", "sn_add", "min_price", "maxOnlineView", "uv")
////      vipDF = sqlContext.sql(sql_vip).selectExpr("promotion_id", "room_id", "live_id", "time_t", "sn_add", "min_price", "maxOnlineView", "uv")
////    }
////    //    union fixDF
////    allDF.printSchema()
////    allDF = allDF.selectExpr("*", "concat_ws('_',concat_ws('_',promotion_id,room_id) ,live_id) AS vip_key")
//////
//////    allDF.show()
//////    vipDF.show()
////    //    处理 allDF 和 vipDF
////    val vipKeyList = vipDF.selectExpr("promotion_id", "room_id", "live_id").dropDuplicates("promotion_id", "room_id", "live_id").rdd
////      .map(e => e.getAs[String]("promotion_id") + "_" + e.getAs[String]("room_id") + "_" + e.getAs[String]("live_id")).collect()
////    val vipKeyStr = "('" + vipKeyList.mkString("','") + "')"
////
////    allDF = allDF.where(s"vip_key not in $vipKeyStr").drop("vip_key").union(vipDF).union(fixDF).orderBy(new Column("room_id"), new Column("maxOnlineView").desc, new Column("live_id"), new Column("time_t"))
////
////    val fixGoodsMap = promotionDF.rdd.map(e => (e.getAs[String]("promotion_id"), e.getAs[Double]("min_price"))).collectAsMap()
////    //    println(fixGoodsMap)
////
////
////    val aggDF = allDF.groupBy("promotion_id", "time_t").agg("sn_add" -> "min", "maxOnlineView" -> "sum").toDF("promotion_id", "time_t", "sn_add_min", "maxOnlineView_sum")
////    //    df_2.show()
////
////    val df = allDF.join(aggDF, Seq("promotion_id", "time_t"), "left")
////    //    df.show()
////    //    df.printSchema()
////    //    System.exit(0)
////    val resDF = df.rdd.groupBy(_.getAs[String]("room_id"))
////      .flatMap(tup => {
////        val arraybuffer = new ArrayBuffer[(String, String, String, Long, Int, Double, Int, Int)](tup._2.size)
////        val room_id = tup._1
////        var maxOnlineView_mid: Any = 0
////        var resTup: (String, String, String, Long, Int, Double, Int, Int) = null
////
////        tup._2.foreach(r => {
////          var sn_add = r.getAs[Any]("sn_add").toString.toInt
////          val sn_add_origin = sn_add
////          val sn_add_min = r.getAs[Any]("sn_add_min").toString.toInt
////          val maxOnlineView_sum = r.getAs[Long]("maxOnlineView_sum") * maxOnlineView_percent
////          val promotion_id = r.getAs[String]("promotion_id")
////          var min_price = r.getAs[Double]("min_price")
////          var maxOnlineView = r.get(6) // row.getAs[Int]("maxOnlineView")
////
////          if (maxOnlineView != null && maxOnlineView != 0) maxOnlineView_mid = maxOnlineView
////          else maxOnlineView = maxOnlineView_mid
////
////          if (maxOnlineView.toString.toInt * (maxOnlineView_percent + 0.2) < maxOnlineView_sum) {
////            if (maxOnlineView_sum != 0) sn_add = (sn_add_min * (maxOnlineView.toString.toInt / maxOnlineView_sum)).toInt
////            else sn_add = 0
////          }
////
////          if (fixGoodsMap.contains(promotion_id)) min_price = fixGoodsMap(promotion_id)
////
////          var sn_add_new: Double = 0.0
////          if (min_price > 100) {
////            if (maxOnlineView.toString.toInt >= 100000) sn_add_new = maxOnlineView.toString.toInt / sn_add_percent_10W
////            else if (maxOnlineView.toString.toInt >= 10000) sn_add_new = maxOnlineView.toString.toInt / sn_add_percent_1W
////            else if (maxOnlineView.toString.toInt >= 5000) sn_add_new = maxOnlineView.toString.toInt / sn_add_percent_5000
////            else sn_add_new = maxOnlineView.toString.toInt / sn_add_percent
////          } else {
////            if (sn_add > maxOnlineView.toString.toInt) sn_add_new = maxOnlineView.toString.toInt
////            else sn_add_new = sn_add
////          }
////          if (sn_add_new < sn_add) sn_add = sn_add_new.toInt
////
////          resTup = (r.getAs[String]("promotion_id"), room_id, r.getAs[String]("live_id"),
////            r.getAs[Long]("time_t"), sn_add, min_price, maxOnlineView.toString.toInt, sn_add_origin)
////
////          arraybuffer += resTup
////        })
////        arraybuffer
////      })
////      .toDF("promotion_id", "room_id", "live_id", "time_t", "sn_add", "min_price", "maxOnlineView", "sn_add_origin")
////    //        resDF.show()
////    //        System.exit(0)
////    resDF
////  }
////}
////
