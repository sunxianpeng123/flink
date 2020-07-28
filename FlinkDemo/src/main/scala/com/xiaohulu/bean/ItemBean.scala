package com.xiaohulu.bean

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
  var uid = ""
  //fans list have
  var nickname = ""
  var display_id = ""

  var time = ""
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
  var liveId = ""
  //fans list have
  var totalViewer = 0

  val onlineViewer = 0
  var dySceneValue = 0
  val dyValue = 0
  val dyValueIn = 0
  var count = 0

  var fansList: Array[FansListFansBean] = Array.empty
  var from = ""
  var current_promotion_id = ""
  var goodsInfoList: Array[GoodsInfoBean] = Array.empty
  // purchase intention
  var fromname = ""

  var personNumber = 0
  var content = ""
  var msgId:String  = _
  var subType:Int = _
  var gift_id :String = _

  var giftCount:Int = _
  var giftPrice:String = ""
  var giftOldPrice:String = ""
  var giftName:String = _
  var giftType :String = _
  var fromId :String = ""


  //todo 淘宝数据独有字段
  var plat: Int = _
  var room_id: String = _
  var PV: Long = _
  var timestamp: String = _
  var diggCount: Int = _
  var field: String = _
  var topic = ""
  //todo 快手数据独有字段
  var PlatId: Int = _
  var LiveId: String = _
  var PlayStartTime: Long = _
  var PlayEndTime: Long = _
  var Title: String = _
  var AuthorId: String = _
  var NickName: String = _
  var UserId: String = _
  var Eid: String = _
  var CoverUrl: String = _
  var WatchCount: Int = _
  var TimeStamp: String = _
  var LikeUserCount: Int = _
  var SerialId: Long = _


  override def toString = s"ItemBean(${goodsInfoList.mkString(",")})"
}
