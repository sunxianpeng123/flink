package com.xiaohulu.bean.analysisBean

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
  var in_stock: Boolean = _

  var stock_num: Int = _
  var cos_fee: Int = _
  var cos_ratio: Int = _
  var sales_number: Int = _
  var price: Int = _

  var min_price: Int = _
  var index: Int = _
  var platform_label = ""
  var shop_id = ""
  var title = ""

  var short_title = ""
  var platform: Int = _
  var item_type = ""
  var detail_url = ""
  var images: Array[String] = Array.empty
  //new added
  var platform_sales_number: Long = _
  var coupon: Long = _
  var seckill_start_time: Long = _
  var seckill_end_time: Long = _
  var seckill_title = ""

  var seckill_left_stock: Long = _
  var seckill_stock: Long = _
  var seckill_min_price: Double = _
  var seckill_max_price: Double = _
  var tb_categoryId = ""
  var tb_rootCategoryId = ""
  var platform_price: Int = _
  var views: Int = _
  var promote_remark: String = _
  //

  //todo 淘宝数据独有字段
  var plat: Int = _
  var live_id: String = _
  var room_id: String = _
  var timestamp: Long = _
  var item_remark: String = _


  var current_promotion_id: String = _
  //  var tb_categoryid: String = _
  //  var tb_rootcategoryid: String = _

  //todo 快手数据独有字段
  var PlatId: Int = _
  var localId: String = _
  var id: String = _
  var LiveId: String = ""
  var userId: String = _

  var imageUrl: String = _
  var currentStock: Int = _
  var soldNum: Int = _
  var displayPrice: Long = _
  var localPrice: Long = _

  var itemType: String = _
  var sequence: Int = _
  var shopId: String = _
  var jumpUrl: String = _
  var localTitle: String = _

  var timeStamp: String = _
  var detail: String = _
  var KillStartTime: Long = _
  var KillEndTime: Long = _
  var KillPrice: Long = _

  var KillOriginVolume: Long = _
  var KillVolume: Long = _
  var tb_categoryid = ""
  var tb_rootcategoryid = ""

  //  var goodsId: String = _
  //  var jumpToken: String = _
  //  var localShopId: String = _
  //  var express: Int = _
  //  var SerialId: String = _
  //  var KillId: String = _
}
