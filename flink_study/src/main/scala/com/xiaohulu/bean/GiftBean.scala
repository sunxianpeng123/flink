package com.xiaohulu.bean

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/7/8
  * \* Time: 16:58
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
class GiftBean  extends  Serializable {
//
//  .selectExpr("count * price as gift_price", "from_id", "from_name", "gift_name", "gift_type", "room_id", "timestamp","platform_id as plat")
  var platform_id :String =""
  var room_id :String = ""

  var from_id :String =""
  var from_name:String = ""

//  var gift_name:String = ""
  var gift_type :String = ""
  var price:Double = 0.0
  var count:Int = 0

  var timestamp:String = ""
  var date:String = ""

  var gift_id = ""
  var gift_name=""


  var priceold=""
  var gift_img=""
  var gift_mimg=""
  var sourcelink=""




}
