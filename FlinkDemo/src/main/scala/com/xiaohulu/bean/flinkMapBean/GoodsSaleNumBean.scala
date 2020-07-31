package com.xiaohulu.bean.flinkMapBean

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/28
  * \* Time: 18:28
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
class GoodsSaleNumBean extends Serializable {
  var platform_id :String = _
  var room_id: String = _
  var promotion_id: String = _
  var live_id: String = _
  var timestamp: Long = _
  var sales_number: Long = _

  override def toString = s"GoodsSaleNumBean(room_id=$room_id, promotion_id=$promotion_id, live_id=$live_id, timestamp=$timestamp, sales_number=$sales_number)"
}
