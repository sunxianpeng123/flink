package com.xiaohulu.demo.test.bean

import com.fasterxml.jackson.annotation.JsonIgnoreProperties


/**
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
