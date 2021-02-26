package com.xiaohulu.demo.test.bean

import com.fasterxml.jackson.annotation.JsonIgnoreProperties


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
