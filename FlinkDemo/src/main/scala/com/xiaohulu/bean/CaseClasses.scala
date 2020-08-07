package com.xiaohulu.bean

/**
  * @author xq
  * @date 2020/7/29 18:59
  * @version 1.0
  * @describe:
  */
//class CaseClasses {
//
//}

case class keyTrans(promotionPId: String, salesNumber: Int, minPrice: Double, timeStamp: Long, date: String)

case class GoodsTrans(promotionId: String, roomId: String, liveId: String, salesNumber: Int, minPrice: Double, timeStamp: Long, date: String)

case class GoodsTransRes(promotionId: String, roomId: String, liveId: String, salesNumber: Int, incrNumber: Int, minPrice: Double, timeStamp: Long, date: String)

//                        主播id     场次id           总人气          在线人气
case class AnchorTrans(roomId: String, liveId: String, totalViewer: Int, onlineViewer: Int, timeStamp: Long, date: String)

/**
  * 统计货物数据，每个窗口下最大timestamp 的 货物的 sales_number ，该class 为 返回结果
  *
  * @param platform_id
  * @param room_id
  * @param promotion_id
  * @param live_id
  * @param timestamp
  * @param sales_number
  */
case class GoodsSaleNumBean(var platform_id: String, var room_id: String, var promotion_id: String, var live_id: String, var timestamp: Long, var sales_number: Int) extends Serializable

/**
  * 统计窗口下，货物的 如下字段的 最大值或者最小值，该class 为 返回结果
  *
  * @param platform_id
  * @param promotion_id
  * @param max_seckill_min_price
  * @param min_min_price
  * @param max_coupon
  * @param max_promote_remark
  */
case class GoodsPromotionAggBean(platform_id: String, promotion_id: String, max_seckill_min_price: Double, min_min_price: Double, max_coupon: Double, max_promote_remark: String) extends Serializable


