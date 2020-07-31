package com.xiaohulu.adapter

import com.xiaohulu.transform.udf.RegexpExtractAll
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.Max

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/27
  * \* Time: 18:31
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object KsAdapter {

  def transGoodsInfo(dyAnchorDataStream: Table, dyGoodsDataStream: Table, tEnv: StreamTableEnvironment): Unit = {
    //注册用户自定义函数
    tEnv.registerFunction("regexp_extract_all", new RegexpExtractAll())
    //创建虚拟表
    val anchor_basic_tb_name = "anchor_basic_tb_name"
    val goods_tb_name = "good_info_tb"
    tEnv.createTemporaryView(anchor_basic_tb_name, dyAnchorDataStream)
    tEnv.createTemporaryView(goods_tb_name, dyGoodsDataStream)

    val sql_all =
      s"""  select d.*, maxOnlineView,uv  from(select promotion_id, room_id ,live_id, floor(timestamp/300)*300 as time_t ,sum(sn_add) as sn_add , min(mp)  as min_price  from (
          select promotion_id, live_id, room_id, timestamp  , lead(sn,1,sn) over (PARTITION BY promotion_id,room_id, live_id order by promotion_id, room_id, live_id,timestamp) -sn as sn_add ,sn ,mp from(
          select  timestamp ,room_id, t.promotion_id ,live_id, salesNum as sn  ,mp   from (
          SELECT timestamp ,room_id, promotion_id,live_id, max(sales_number) as salesNum  FROM $goods_tb_name v  group by  promotion_id , room_id, live_id, timestamp) t
            inner join (select promotion_id,if(mp >max_coupon,mp -max_coupon ,mp ) as mp from (
            select promotion_id,max_coupon,if(mp >= standard,mp-discounts,mp) as mp from (
            select promotion_id,mp,max_coupon,if(discounts_price is not null,cast(discounts_price[0] as double)*100.0,0.0) as standard,if(discounts_price is not null,cast(discounts_price[1] as double)*100.0,0.0) as discounts from(
            select promotion_id,mp,max_coupon,if(promote_remark_max like '满%元,省%元',regexp_extract_all(promote_remark_max),null) as discounts_price from (
            select  promotion_id,IF(max(seckill_min_price)>0, max(seckill_min_price), min(min_price)) as mp,max(coupon) as max_coupon  , max(promote_remark) as promote_remark_max FROM  $goods_tb_name v  group by  promotion_id))))
            )m on t.promotion_id= m.promotion_id where mp > 0
          ) b  order by promotion_id, room_id , live_id,timestamp) v
          where sn_add != 0
            group by  promotion_id, room_id ,live_id,floor(timestamp/300)*300  order by      room_id,    live_id, floor(timestamp/300)*300 ) d
            left join (select room_id ,floor(timestamp/300)*300 as time_t , max(onlineviewer) as maxOnlineView, max(totalviewer) as uv from $anchor_basic_tb_name
            group by room_id ,floor(timestamp/300)*300 ) a  on a.room_id=d.room_id  and a.time_t=d.time_t where sn_add > 0  order by      room_id,   maxOnlineView desc ,  live_id, time_t"""
    println(sql_all)
    val sql_vip =
      s"""select d.*, maxOnlineView,uv  from(select promotion_id, room_id ,live_id, floor(timestamp/300)*300 as time_t ,sum(sn_add) as sn_add , min(mp)  as min_price, min(max_sales) from (
                    select promotion_id, live_id, room_id, timestamp  , lead(sn,1,sn) over (PARTITION BY promotion_id,room_id, live_id  order by promotion_id, room_id, live_id,timestamp) -sn as sn_add   ,sn ,mp  ,max_sales from(
                    select  timestamp ,room_id, t.promotion_id ,live_id, platform_sales_num as sn  ,mp ,max_sales from (
                    SELECT timestamp ,room_id, promotion_id,live_id, max(sales_number) as salesNum  ,max(platform_sales_num)  as platform_sales_num  FROM $goods_tb_name v
                     WHERE   platform_sales_num>0  and floor(min_price/100) not in (9999999,999999,99999)   group by  promotion_id , room_id, live_id, timestamp) t
                    inner join (select promotion_id,max_sales,if(mp >max_coupon,mp -max_coupon ,mp ) as mp from (
                    select promotion_id,max_coupon,max_sales,standard,discounts,if(mp >= standard,mp-discounts,mp) as mp from (
                    select promotion_id,mp,max_coupon,max_sales,if(discounts_price is not null,cast(discounts_price[0] as double)*100.0,0.0) as standard,if(discounts_price is not null,cast(discounts_price[1] as double)*100.0,0.0) as discounts from (
                    select promotion_id,mp,max_coupon,max_sales,if(promote_remark_max like '满%元,省%元',regexp_extract_all(promote_remark_max),null) as discounts_price from (
                    select  promotion_id,IF(max(seckill_min_price)>0, max(seckill_min_price),  IF(min(platform_price)>0,min(platform_price) ,min(min_price) ))  as mp,max(coupon) as max_coupon  , max(promote_remark) as promote_remark_max,max(sales_number)-min(sales_number) as  max_sales FROM $goods_tb_name v WHERE   platform_sales_num>0  group by  promotion_id))))
                    )m on t.promotion_id= m.promotion_id  where mp > 0 ) b
                    order by promotion_id, room_id , live_id,timestamp) v where sn_add != 0
                    group by  promotion_id, room_id ,live_id,floor(timestamp/300)*300  order by  room_id, live_id, floor(timestamp/300)*300 ) d
                    left join (select room_id ,floor(timestamp/300)*300 as time_t , max(onlineviewer) as maxOnlineView, max(totalviewer) as uv
                    from $anchor_basic_tb_name   group by room_id ,floor(timestamp/300)*300 ) a
                    on a.room_id=d.room_id  and a.time_t=d.time_t  where sn_add >0  order by   promotion_id, room_id,   time_t   """

    val testsql_1 =
      s"""select promotion_id,if(mp >max_coupon,mp -max_coupon ,mp ) as mp from (
                          select promotion_id,max_coupon,if(mp >= standard,mp-discounts,mp) as mp from (
                            select promotion_id,mp,max_coupon,if(discounts_price is not null,cast(discounts_price[0] as double)*100.0,0.0) as standard,if(discounts_price is not null,cast(discounts_price[1] as double)*100.0,0.0) as discounts from(
                            select promotion_id,mp,max_coupon,if(promote_remark_max like '满%元,省%元',regexp_extract_all(promote_remark_max),null) as discounts_price from (
                            select  promotion_id,IF(max(seckill_min_price)>0, max(seckill_min_price), min(min_price)) as mp,max(coupon) as max_coupon  , max(promote_remark) as promote_remark_max FROM  $goods_tb_name v  group by  promotion_id))))"""


    val testtable_1 = tEnv.sqlQuery(s"select if(max(seckill_min_price)>0, max(seckill_min_price), min(min_price)) as mp FROM  $goods_tb_name v  group by  promotion_id")
    testtable_1.printSchema()
    testtable_1.toRetractStream[Double].print()









    //    val allTb = tEnv.sqlQuery(sql_all)
    //    allTb.select("promotion_id",'room_id)
    //      .select("promotion_id", "room_id", "live_id", "time_t", "sn_add", "min_price", "maxOnlineView", "uv")
    //    allTb.printSchema()
    //
    //    val vipTb = tEnv.sqlQuery(sql_vip)
    //    vipTb.printSchema()
    //      .selectExpr("promotion_id", "room_id", "live_id", "time_t", "sn_add", "min_price", "maxOnlineView", "uv")

//
//    val testAnchor = tEnv.sqlQuery(s"select * from $anchor_basic_tb_name")
//    val testGoods = tEnv.sqlQuery(s"select * from $goods_tb_name")
//
//
//
//    //    testAnchor.toAppendStream[AnchorResultBean].print()//批转流
//    testGoods.printSchema()
//    testGoods.toAppendStream[GoodsResultBean].print() //批转流

  }
}

