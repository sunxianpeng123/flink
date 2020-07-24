//package com.xiaohulu.tools
//
//import com.google.gson.Gson
//
//import com.xiaohulu.conf.ConfigTool
//
//import scala.util.control.Breaks._
//import scalaj.http.{Http, HttpResponse}
//import scala.collection.JavaConverters._
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: sunxianpeng
//  * \* Date: 2020/6/11
//  * \* Time: 17:36
//  * \* To change this template use File | Settings | File Templates.
//  * \* Description:
//  * \*/
//object HttpTool {
//
//  val tryTimes = 5 //重试次数
//  /**
//    * 读取 SVIP 主播
//    *
//    * @return
//    */
//  def getSvipInfoBean(url:String): String = {
//    var svipRoomIds = ""
//    val gs = new Gson()
//    var status: Int = -1
//    var response: HttpResponse[String] = null
//    var beginTime = 0
//    //开始是第几次
//    var waitTryNextTime = 5000 //等待时间ms
//    try {
//      //      svip_url=http://121.10.141.53:9995/api/v1/douyin/online/svip/view?level=1
//
//      breakable {
//        while (beginTime < tryTimes) {
//          try {
//            response = Http(url).timeout(5000, 25000).asString
//            status = response.statusLine.split(" ")(1).toInt
//            //            println(response)
//            //            println(status)
//          } catch {
//            case e: Exception => {
//              println(s"===========第" + (beginTime + 1) + "尝试读取 SVIP 主播========")
//              Thread.sleep(waitTryNextTime)
//              waitTryNextTime += 5000
//            }
//          }
//          if (response != null && status >= 200 && status <= 300) {
//            println(break())
//            break
//          }
//          beginTime += 1
//        }
//      }
//    } catch {
//      case e: Exception => {
//        println("GetSvipInfo Error!!")
//        e.printStackTrace()
//      }
//    }
//    //    解析 json,构建 sql 使用的格式
//    if (response != null) {
//      val svip_json = response.body
//      val bean = gs.fromJson(svip_json, classOf[SvipBean])
//      svipRoomIds = bean.data.list.asScala.map(_.uid).mkString(",")
//      //      println(svipRoomIds)
//    }
//    svipRoomIds
//  }
//
//
//}
