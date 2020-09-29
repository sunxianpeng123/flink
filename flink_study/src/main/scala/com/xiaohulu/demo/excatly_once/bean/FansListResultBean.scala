package com.xiaohulu.demo.excatly_once.bean

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Created by xiangjia on 2016/12/28 0028.
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
