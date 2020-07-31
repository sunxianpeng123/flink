package com.xiaohulu.bean.analysisBean

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/7/8
  * \* Time: 16:58
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
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
