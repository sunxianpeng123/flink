package com.xiaohulu.demo.test.bean

/**
  * @author xq
  * @date 2020/7/29 18:59
  * @version 1.0
  * @describe:
  */
class CaseClasses {

}

case class keyTrans(promotionPId:String,salesNumber:Int,minPrice:Double,timeStamp:Long,date:String)
case class GoodsTrans(promotionId:String,roomId:String,liveId:String,salesNumber:Int,minPrice:Double,timeStamp:Long,date:String)
case class GoodsTransRes(promotionId:String,roomId:String,liveId:String,salesNumber:Int,incrNumber:Int,minPrice:Double,timeStamp:Long,date:String)
//                        主播id     场次id           总人气          在线人气
case class AnchorTrans(roomId:String,liveId:String,totalViewer:Int,onlineViewer:Int,timeStamp:Long,date:String)


case class GoodsAnchorRes(roomId:String,liveId:String,promotionId:String,timeMinut:String,salesNumber:Integer,minPrice:Double,incrSales:Integer,totalViewer:Integer,onlineViewer:Integer,timestamp:String)


case class GoodsTransMid(promotionId:String,roomId:String,liveId:String,salesNumber:Int,minPrice:Double,timeStamp:Long)

case class GoodsResM(room_id:String,promotion_id :String, live_id :String,min_price:Double,sales_num:Int,increase_num:Int,trend:String,live_inc:Int)

case class GoodsTransMid2(platformId:String,roomId:String,promotionId:String,liveId:String,productId:String,inStock:Boolean,
                          stockNum:Int,cosFee:Double,cosRatio:Double,platform:Int,salesNumber:Int,cover:String,isVirtual:String,
                          price:Double,minPrice:Double,platformLabel:String,itemType:String,shopId:String,detailUrl:String,title:String,
                          shortTitle:String,timeStamp:Long)

case class TransAcc(platformId:String,roomId:String,promotionId:String,liveId:String,productId:String,inStock:Boolean,
                    stockNum:Int,cosFee:Double,cosRatio:Double,platform:Int,salesNumber:Int,cover:String,isVirtual:String,
                    price:Double,minPrice:Double,platformLabel:String,itemType:String,shopId:String,detailUrl:String,title:String,
                    shortTitle:String,timeStamp:Long,inc:Int,endDate:String)

case class TransAccRes(platformId:Int,roomId:String,promotionId:String,liveId:String,productId:String,inStock:Boolean,
                    stockNum:Int,cosFee:Double,cosRatio:Double,platform:Int,salesNumber:Int,cover:String,isVirtual:String,
                    price:Double,minPrice:Double,platformLabel:String,itemType:String,shopId:String,detailUrl:String,title:String,
                    shortTitle:String,inc:Int,trend:String,startDate:String,endDate:String,onSaleTime:String)