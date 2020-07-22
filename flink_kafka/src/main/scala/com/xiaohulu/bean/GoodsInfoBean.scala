package com.xiaohulu.bean

import java.sql.Timestamp

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Created by xiangjia on 2016/12/28 0028.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class GoodsInfoBean extends Serializable {

  var product_id = ""
  var promotion_id = ""
  var cover = ""
  var is_virtual = ""
  var in_stock :Boolean = _

  var stock_num:Int = _
  var cos_fee :Int= _
  var cos_ratio:Int = _
  var sales_number:Int = _
  var price:Int = _

  var min_price :Int=_
  var index:Int = _
  var platform_label = ""
  var shop_id = ""
  var title = ""

  var short_title = ""
  var platform :Int = _
  var item_type = ""
  var detail_url = ""
  var images:Array[String] = Array.empty
  //new added
  var platform_sales_number:Long = _
  var coupon :Long = _
  var seckill_start_time :Long = _
  var seckill_end_time :Long = _
  var seckill_title = ""

  var seckill_left_stock :Long = _
  var seckill_stock :Long = _
  var seckill_min_price :Double = _
  var seckill_max_price :Double = _

  var time:Long=_
  var room_id=""
  var live_id=""
  var row_time=0l
  var eventAccTime:Timestamp=_

}
