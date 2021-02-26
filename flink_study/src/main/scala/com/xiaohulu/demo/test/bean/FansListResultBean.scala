package com.xiaohulu.demo.test.bean

import com.fasterxml.jackson.annotation.JsonIgnoreProperties


/**
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class FansListResultBean extends Serializable {

  var platformId = ""
  var room_id = ""
  var from_id = ""
  var live_id = ""
  var nickname = ""

  var display_id = ""
  var secId = ""
  var secret = ""
  var head = ""
  var gender = ""

  var dyCoinOut = 0
  var rank = 0
  var location = ""
  var timestamp = ""
  var date = ""
  var fansClubLevel = "" //粉丝团等级
  var payLevel = "" //付费等级
  var fansClubName = "" //粉丝团名字

}
