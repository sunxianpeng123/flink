package com.xiaohulu.demo.excatly_once.bean

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Created by xiangjia on 2016/12/28 0028.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class FansInfoResultBean extends Serializable {

  var platformId = ""
  var from_id = ""
  //
  var nickname = ""
  var display_id = ""
  var secId = ""
  var secret = ""

  //gift
  var head = ""
  var gender = ""

  var dyCoinOut = 0
  var level = ""
  var dyValueIn = 0
  var introduce = ""


  var fansCount = 0
  var followCount = 0
  var location = ""
  var timestamp = ""
  var date = ""

}
