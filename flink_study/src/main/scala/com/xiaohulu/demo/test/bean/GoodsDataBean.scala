package com.xiaohulu.demo.test.bean

/**
  * @author xq
  * @date 2020/12/15 14:57
  * @version 1.0
  * @describe:
  */
class GoodsDataBean extends Serializable{

  var version :String = ""
  var sid :String = ""
  var topic :String = ""
  var time :Long = _
  var ip :String =""
  var roomid:String = ""
  var level:Int = _
  var sourcelink:String = ""
  var count :Int = _

  var item:Array[ItemBean] = Array.empty

}
