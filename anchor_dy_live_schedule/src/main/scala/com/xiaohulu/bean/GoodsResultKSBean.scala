package com.xiaohulu.bean

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Created by xiangjia on 2016/12/28 0028.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class GoodsResultKSBean extends Serializable {
  var platform_id =""
  var product_id = ""
  var promotion_id = ""
  var live_id = ""
  var room_id = ""

  var current_promotion_id = ""
  var cover = ""
  var stock_num:Int = _
  var sales_number :Int= _
  var min_price :Double=_

  var platform_price:Double = _
  var platform_label = ""
  var item_type = ""
  var index:Int = _
  var shop_id = ""

  var detail_url = ""
  var title = ""
  var short_title = ""
  var item_remark :String = _
  var seckill_start_time :Long = _

  var seckill_end_time :Long = _
  var seckill_min_price :Double = _
  var seckill_start_sales_number:Long = _
  var seckill_sales_number:Long = _
  var timestamp = ""

  var tb_categoryId = ""
  var tb_rootcategoryId = ""
  var promote_remark:String = _

  var date = ""

}
