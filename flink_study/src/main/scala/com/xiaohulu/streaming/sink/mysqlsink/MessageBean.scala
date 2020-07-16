package com.xiaohulu.streaming.sink.mysqlsink

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/7/8
  * \* Time: 16:58
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
class MessageBean extends  Serializable {
//  .selectExpr("content", "from_id", "room_id", "timestamp","platform_id as plat")
  var platform_id :String = ""
  var room_id :String = ""

  var from_id :String =""
  var content :String = ""

  var msg_count = 1
//  var timestamp:String = ""
//  var date:String = ""

  override def toString = s"MessageBean(platform_id=$platform_id, room_id=$room_id, from_id=$from_id, content=$content, msg_count=$msg_count)"
}
