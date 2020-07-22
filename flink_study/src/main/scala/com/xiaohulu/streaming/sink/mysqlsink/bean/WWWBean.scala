package com.xiaohulu.streaming.sink.mysqlsink.bean

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/7/8
  * \* Time: 16:58
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
//@JsonIgnoreProperties(ignoreUnknown = true)
class WWWBean extends  Serializable {
  var url = ""
  var xhlid :String = ""
  var time :Long = 0l
  var ip :String =""
  var input :Any=_
  var row_time :Long = 0l


}
