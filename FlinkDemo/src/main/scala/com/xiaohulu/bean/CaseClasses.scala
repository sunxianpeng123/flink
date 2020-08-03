package com.xiaohulu.bean

/**
  * @author xq
  * @date 2020/7/29 18:59
  * @version 1.0
  * @describe:
  */
//class CaseClasses {
//
//}

case class keyTrans(promotionPId:String,salesNumber:Int,minPrice:Double,timeStamp:Long,date:String)
case class GoodsTrans(promotionId:String,roomId:String,liveId:String,salesNumber:Int,minPrice:Double,timeStamp:Long,date:String)
case class GoodsTransRes(promotionId:String,roomId:String,liveId:String,salesNumber:Int,incrNumber:Int,minPrice:Double,timeStamp:Long,date:String)
//                        主播id     场次id           总人气          在线人气
case class AnchorTrans(roomId:String,liveId:String,totalViewer:Int,onlineViewer:Int,timeStamp:Long,date:String)


