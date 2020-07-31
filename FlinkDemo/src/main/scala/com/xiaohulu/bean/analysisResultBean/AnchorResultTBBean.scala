package com.xiaohulu.bean.analysisResultBean

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
  * Created by xiangjia on 2016/12/28 0028.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
class AnchorResultTBBean extends Serializable {
  var platformId = ""
  var room_id = ""
  var liveId = ""
  var nickname = ""
  var display_id = ""

  var head = ""
  var gender = ""
  var introduce = ""
  var level = ""
  var totalViewer = 0

  var onlineViewer = 0
  var PV: Long = _
  var fansCount = 0
  var location = ""
  var cover = ""

  var title = ""
  var diggCount = 0
  var field = ""
  var timestamp = ""
  var topic = ""

  var date = ""


}
