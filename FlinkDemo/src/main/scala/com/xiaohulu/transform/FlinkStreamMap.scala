package com.xiaohulu.transform

import java.text.SimpleDateFormat
import java.util.Date

import com.google.gson.{Gson, JsonParser}
import com.xiaohulu.bean.analysisBean.DataBean
import com.xiaohulu.bean.analysisResultBean.{AnchorResultBean, GoodsResultBean}
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.JavaConverters._
import org.apache.flink.streaming.api.scala._
/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/24
  * \* Time: 16:53
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object FlinkStreamMap {
  /**
    * 解析抖音主播基础数据kafka数据流
    * @param dataStream
    */
  def analysisDyAnchorKafkaStream(dataStream:DataStream[String]):DataStream[AnchorResultBean]={

    val dyAnchorDataStream = dataStream.map(line=>{
      val anchorBasicMark = "scene_basic"
      val jsonParse = new JsonParser()
      val gs = new Gson()
      val sdf = new SimpleDateFormat("yyyyMMdd")
      var dataArr: Array[DataBean] = Array.empty
      var anchorBasicInfoArr: Array[AnchorResultBean] = Array.empty
      try{
        val je = jsonParse.parse(line)
        if (je.isJsonArray) dataArr = gs.fromJson(je, classOf[Array[DataBean]])
        else dataArr :+= gs.fromJson(je, classOf[DataBean])

        dataArr.foreach(databean=>{
          var type_name = ""
          var platID = ""
          var time = ""
          var itemTime = ""

          if (databean != null & databean.item != null & databean.item.length > 0) {
            type_name = databean.type_name
            platID = databean.sid
            time = databean.time
            var anchorBasicBean: AnchorResultBean = null
            databean.item.foreach(item => {
              itemTime = item.time
              anchorBasicBean = new AnchorResultBean
              if (item != null) {
                //todo 抖音数据
                if (item.typeName.equals(anchorBasicMark)) {
                  //主播基础信息数据
                  anchorBasicBean.platformId = platID;
                  anchorBasicBean.room_id = item.uid;
                  anchorBasicBean.liveId = item.liveId
                  anchorBasicBean.nickname = item.nickname;
                  anchorBasicBean.display_id = item.display_id;

                  anchorBasicBean.secId = item.secId;
                  anchorBasicBean.secret = item.secret;
                  anchorBasicBean.head = item.head;
                  anchorBasicBean.gender = item.gender;
                  anchorBasicBean.introduce = item.introduce;

                  anchorBasicBean.level = item.level;
                  anchorBasicBean.totalViewer = item.totalViewer
                  anchorBasicBean.onlineViewer = item.onlineViewer
                  anchorBasicBean.dySceneValue = item.dySceneValue
                  anchorBasicBean.dyValue = item.dyValue;

                  anchorBasicBean.dyCoinOut = item.dyCoinOut;
                  anchorBasicBean.fansCount = item.fansCount;
                  anchorBasicBean.followCount = item.followCount;
                  anchorBasicBean.location = item.location;
                  anchorBasicBean.title = item.title;

                  anchorBasicBean.cover = item.cover;
                  if (itemTime.equals("")) anchorBasicBean.timestamp = time
                  else anchorBasicBean.timestamp = itemTime
                  val date = sdf.format(new Date(anchorBasicBean.timestamp.toLong * 1000))
                  anchorBasicBean.date = date

                  anchorBasicInfoArr :+= anchorBasicBean
                }
              }
            })

          }
        })
      }catch {
        case e:Exception =>println(e.printStackTrace())
      }
      anchorBasicInfoArr
    })
      .flatMap(e=>e)
    dyAnchorDataStream
  }

  /**
    * 解析抖音货物数据kafka数据流
    * @param dataStream
    */
  def analysisDyGoodsKafkaStream(dataStream:DataStream[String]):DataStream[GoodsResultBean]={

    val dyAnchorDataStream = dataStream.map(line=>{
      val goodsMark = "scene_goods_infos"
      val jsonParse = new JsonParser()
      val gs = new Gson()
      val sdf = new SimpleDateFormat("yyyyMMdd")
      var dataArr: Array[DataBean] = Array.empty
      var goodsInfoArr: Array[GoodsResultBean] = Array.empty
      try{
        val je = jsonParse.parse(line)
        if (je.isJsonArray) dataArr = gs.fromJson(je, classOf[Array[DataBean]])
        else dataArr :+= gs.fromJson(je, classOf[DataBean])

        dataArr.foreach(databean=>{
          var type_name = ""
          var platID = ""
          var time = ""
          var itemTime = ""
          if (databean != null & databean.item != null & databean.item.length > 0) {
            type_name = databean.type_name
            platID = databean.sid
            time = databean.time
            var goodsResultBean: GoodsResultBean = null
            databean.item.foreach(item => {
              itemTime = item.time
              if (item != null) {
                //todo 抖音数据
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
                    if (itemTime.equals("")) goodsResultBean.timestamp = time else goodsResultBean.timestamp = itemTime
                    goodsResultBean.date = sdf.format(new Date(goodsResultBean.timestamp.toLong * 1000))
                    goodsResultBean.promote_remark = listbean.promote_remark

                    goodsInfoArr :+= goodsResultBean
                  })
                }
              }
            })

          }
        })
      }catch {
        case e:Exception =>println(e.printStackTrace())
      }
      goodsInfoArr
    }).flatMap(e=>e)
    dyAnchorDataStream
  }
}

