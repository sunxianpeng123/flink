package com.xiaohulu.mysqlsink

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
  * Created by xiangjia on 2016/12/28 0028.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
class DateBean extends Serializable {
  var item:Array[ItemBean] = Array.empty
  var sid = ""
  var roomid = ""
  var sourcelink=""
}
