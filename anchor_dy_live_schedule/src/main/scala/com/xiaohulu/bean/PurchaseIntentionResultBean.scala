package com.xiaohulu.bean

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/5/9
  * \* Time: 15:18
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
@JsonIgnoreProperties(ignoreUnknown = true)
class PurchaseIntentionResultBean extends Serializable{
  var platform_id =""
  var room_id = ""
  var from_name = ""
  var person_number = 0//购买人数
  var content = ""

  var timestamp = ""
  var msgId:String  = _
  var subType:Int = _//1 购买意向，2 弹幕，3 礼物
  var gift_id :String = _
  var giftCount:Int = _

  var giftPrice:Double = _
  var giftOldPrice:Double = _
  var giftName:String = _
  var giftType :String = _
  var liveId :String = _

  var fromId :String = ""

  var date = ""
}
