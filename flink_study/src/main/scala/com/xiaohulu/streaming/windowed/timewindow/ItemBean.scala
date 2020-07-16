package com.xiaohulu.streaming.windowed.timewindow

import com.google.gson.annotations.SerializedName
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
  * Created by xiangjia on 2016/12/28 0028.
  */
@JsonIgnoreProperties(ignoreUnknown = true)
class ItemBean extends Serializable  {
   @SerializedName("type")
   var typeName = ""
   var roomgroup=""
   var platgroup=""
   var app=""
//
   var fromid=""
   var fromname=""
   var time=""
//message
   var content=""
//gift
   var gift_type=""
   var giftid = ""
   var giftname=""
   var price=""
   var count=0

//   var price_old=""
//   var gift_img=""
//   var gift_mimg=""
//   var sourcelink=""
}
