package com.xiaohulu.demo.test.bean

//@JsonIgnoreProperties(ignoreUnknown = true)
class DataBean extends  Serializable {
//  .selectExpr("content", "from_id", "room_id", "timestamp","platform_id as plat")
  var type_name = ""
  var version :String = ""
  var time :String = ""

  var ip :String =""
  var count :String = ""

  var roomid:String = ""
  var sourcelink:String = ""
  var sid = ""

  var item:Array[ItemBean] = Array.empty

}
