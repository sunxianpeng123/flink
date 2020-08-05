package com.xiaohulu.streaming.windowed.timewindow.bean

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Created by xiangjia on 2016/12/28 0028.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class FansListFansBean extends Serializable {

  var platformId = ""
  var uid = ""
  //
  var display_id = ""
  var nickname = ""
  var secret = ""

  //gift
  var gender = ""
  var head = ""

  var level = ""
  var dyCoinOut = 0

  var rank = 0
  var fansClubLevel = "" //粉丝团等级
  var payLevel = "" //付费等级
  var fansClubName = "" //粉丝团名字


}
