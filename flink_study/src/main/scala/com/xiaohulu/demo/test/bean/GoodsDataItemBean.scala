package com.xiaohulu.demo.test.bean

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.google.gson.annotations.SerializedName


/**
  * @author xq
  * @date 2020/12/15 15:01
  * @version 1.0
  * @describe:
  */
@JsonIgnoreProperties(ignoreUnknown = true)
class GoodsDataItemBean extends Serializable{
  @SerializedName("type")
  var typeName = ""
  var uid = ""
  var liveId = ""
  var count:Int = _
  var from = ""
  var current_promotion_id = ""
  var goodsInfoList: Array[GoodsInfoBean] = Array.empty
  var time :Long = _

}
