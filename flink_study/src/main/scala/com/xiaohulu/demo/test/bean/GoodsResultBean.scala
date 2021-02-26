package com.xiaohulu.demo.test.bean

import com.fasterxml.jackson.annotation.JsonIgnoreProperties


/**
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class GoodsResultBean extends Serializable {
  var platform_id =""

  var product_id = ""
  var promotion_id = ""     //商品唯一id
  var live_id = ""          //最新场次id
  var room_id = ""          //主播id
  var current_promotion_id = ""

  var cover = ""
  var is_virtual = ""
  var in_stock :Boolean = _
  var stock_num:Int = _
  var cos_fee :Double= _

  var cos_ratio:Double = _
  var platform :Int= _
  var sales_number :Int= _  //商品销售量
  var price:Double = _      //价钱
  var min_price :Double=_   //最低价

  var platform_label = ""
  var item_type = ""
  var index:Int = _
  var label_icon = ""
  var shop_id = ""


  var detail_url = ""
  var title = ""
  var short_title = ""
  var images:String = ""
  var timestamp = ""    //时间戳

  var platform_sales_number:Long = _
  var coupon :Long = _
  var seckill_start_time :Long = _
  var seckill_end_time :Long = _
  var seckill_title = ""

  var seckill_left_stock :Long = _
  var seckill_stock :Long = _
  var seckill_min_price :Double = _
  var seckill_max_price :Double = _

  var tb_categoryId = ""
  var tb_rootCategoryId = ""
  var platform_price:Int = _
  var views :Int = _

  var date = ""



}
