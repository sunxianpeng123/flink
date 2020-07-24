package com.xiaohulu.bean

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Created by xiangjia on 2016/12/28 0028.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class AnchorResultBean extends Serializable {
  var platformId = ""
  var room_id = ""
  var liveId = ""


  //
  var nickname = ""
  var display_id = ""
  var secId = ""
  //message
  var secret = ""
  //gift
  var head = ""
  var gender = ""
  var introduce = ""
  var level = ""
  var totalViewer = 0
  var onlineViewer = 0


  var dySceneValue = 0
  var dyValue = 0
  var dyCoinOut = 0
  var fansCount = 0
  var followCount = 0

  var location = ""
  var title = ""
  var cover = ""

  var timestamp = ""
  var date = ""

  override def toString = s"AnchorResultBean(platformId=$platformId, room_id=$room_id, liveId=$liveId, nickname=$nickname, display_id=$display_id)"
}
