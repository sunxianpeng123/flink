package com.xiaohulu.demo.test.bean

import com.fasterxml.jackson.annotation.JsonIgnoreProperties


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
