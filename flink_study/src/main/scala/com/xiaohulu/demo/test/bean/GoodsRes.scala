package com.xiaohulu.demo.test.bean

/**
  * @author xq
  * @date 2020/12/16 10:52
  * @version 1.0
  * @describe:
  */
class GoodsRes extends Serializable{
  var room_id :String = ""
  var promotion_id :String = ""
  var live_id :String = ""
  var min_price :Double = _
  var sales_num :Int = _
  var increase_num:Int = _
  var trend:String = ""
  //var update_time:Timestamp = _

  //override def toString = s"GoodsRes(room_id=$room_id, promotion_id=$promotion_id, live_id=$live_id, min_price=$min_price, sales_num=$sales_num, increase_num=$increase_num)"
  override def toString = s"GoodsRes(min_price=$min_price, sales_num=$sales_num, increase_num=$increase_num, trend=$trend)"


}
