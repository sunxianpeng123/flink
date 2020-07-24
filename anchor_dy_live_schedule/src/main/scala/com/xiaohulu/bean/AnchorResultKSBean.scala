package com.xiaohulu.bean

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
  * Created by xiangjia on 2016/12/28 0028.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
class AnchorResultKSBean extends Serializable {
  var platformId = ""
  var room_id = ""
  var liveId = ""
  var nickname = ""
  var display_id = ""

  var onlineViewer = 0
  var cover = ""
  var title = ""
  var diggCount = 0
  var timestamp = ""

  var date = ""
}
