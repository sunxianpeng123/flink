package com.xiaohulu.streaming.sink.mysqlsink.bean

import com.google.gson.annotations.SerializedName
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Created by xiangjia on 2016/12/28 0028.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class ItemBean extends Serializable {
  @SerializedName("type")
  var typeName = ""
  var platformId = ""
  var uid = ""//fans list have
  var nickname = ""
  var display_id = ""
  var time =0

  var secId = ""
  var secret = ""
  var head = ""
  var gender = ""
  var introduce = ""

  var level = ""
  var dyCoinOut = 0
  val fansCount = 0
  val followCount = 0
  var location = ""

  var title = ""
  var cover = ""

  // anchor 有   but fans info 没有
  var liveId = ""//fans list have
  var totalViewer = 0
  val onlineViewer = 0
  var dySceneValue = 0
  val dyValue = 0

  // fans info 独有的
  val dyValueIn = 0

  //fans List 独有的
  var count = 0
  var fansList:Array[FansListFansBean] = Array.empty

  //
  var from = ""
  var current_promotion_id = ""
  var goodsInfoList:Array[GoodsInfoBean] = Array.empty



  override def toString = s"ItemBean(${goodsInfoList.mkString(",")})"
}
