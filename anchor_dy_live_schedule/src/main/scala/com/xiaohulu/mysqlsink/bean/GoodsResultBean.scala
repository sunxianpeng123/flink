package com.xiaohulu.mysqlsink.bean

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Created by xiangjia on 2016/12/28 0028.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class GoodsResultBean extends Serializable {
  var platform_id =""

  var product_id = ""
  var promotion_id = ""
  var live_id = ""
  var room_id = ""
  var current_promotion_id = ""

  var cover = ""
  var is_virtual = ""
  var in_stock :Boolean = _
  var stock_num:Int = _
  var cos_fee :Int= _

  var cos_ratio:Int = _
  var platform :Int= _
  var sales_number :Int= _
  var price:Double = _
  var min_price :Double=_

  var platform_label = ""
  var item_type = ""
  var index:Int = _
  var label_icon = ""
  var shop_id = ""


  var detail_url = ""
  var title = ""
  var short_title = ""
  var images:String = ""
  var timestamp = ""

  var platform_sales_number:Long = _
  var coupon :Long = _
  var seckill_start_time :Long = _
  var seckill_end_time :Long = _
  var seckill_title = ""

  var seckill_left_stock :Long = _
  var seckill_stock :Long = _
  var seckill_min_price :Double = _
  var seckill_max_price :Double = _
  var date = ""



}
