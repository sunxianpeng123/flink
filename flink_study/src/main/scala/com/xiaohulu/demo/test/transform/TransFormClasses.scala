package com.xiaohulu.demo.test.transform

import java.util

import org.apache.flink.api.common.functions.AggregateFunction

/**
  * @author xq
  * @date 2020/8/6 10:47
  * @version 1.0
  * @describe:
  */
class TransFormClasses {

}

/**
  * 主播信息增量计算
  * (主播id,场次id),总人气,在线人气,时间戳,日期
  * ((String,String),Int,  Int,  Long, String)
  */
class AnchorAggregateFunction() extends AggregateFunction[((String,String),Int,Int,Long,String),
  (String,String,Int,Int,Long,String,Int,Int),(String,String,Int,Int,Long,String)]{
  var map = new util.HashMap[String,String]()
  override def createAccumulator(): (String, String, Int, Int, Long, String, Int, Int) = {
    ("","",0,0,0L,"",0,0)
  }

  override def add(value: ((String, String), Int, Int, Long, String), accumulator: (String, String, Int, Int, Long, String, Int, Int)): (String, String, Int, Int, Long, String, Int, Int) = {
    var mtotalViewer = accumulator._7      //要求的这一分钟内的最大人气
    var monlineViewer = accumulator._8     //要求的这一分钟内的最大人气
    var key = value._1._1+"_"+value._1._2

    /*map.put(key,"1")
    if(arr.length == 0 ){
      mtotalViewer=0
      monlineViewer=0
      arr.+=("fitst")
    }
    */
    if(map.get(key)==null){
      //println("This is null " + key)
      map.put(key,"1")
      mtotalViewer = value._2
      monlineViewer = value._3
      //println("mtotalViewer=" + mtotalViewer + "; monlineViewer="+monlineViewer)
    }

    val roomId = value._1._1
    val liveId = value._1._2
    var timestamp = accumulator._5
    var wintotalViewer = accumulator._3    //本滑动步长内的最大时间人气
    var winonlineViewer = accumulator._4   //本滑动步长内的最大时间在线人数

    var date = accumulator._6
    if(value._4.toLong > timestamp){
      timestamp = value._4
      wintotalViewer = value._2
      winonlineViewer = value._3
      date = value._5
    }
    if(value._2 >= mtotalViewer){
      mtotalViewer = value._2
    }
    if(value._3 >= monlineViewer){
      monlineViewer = value._3
    }
    (roomId,liveId,wintotalViewer,winonlineViewer,timestamp,date,mtotalViewer,monlineViewer)
  }

  override def getResult(acc: (String, String, Int, Int, Long, String, Int, Int)): (String, String, Int, Int, Long, String) = {
    map.clear()
    (acc._1,acc._2,acc._7,acc._8,acc._5,acc._6)
  }

  override def merge(a: (String, String, Int, Int, Long, String, Int, Int), b: (String, String, Int, Int, Long, String, Int, Int)): (String, String, Int, Int, Long, String, Int, Int) = {
    val roomId = a._1
    val liveId = a._2
    var timestamp = a._5
    var wintotalViewer = a._3
    var winonlineViewer = a._4
    var mtotalViewer = a._7
    var monlineViewer = a._8
    var date = a._6
    if(b._5.toLong > timestamp){
      timestamp = b._5
      wintotalViewer = b._3
      winonlineViewer = b._4
      date = b._6
    }
    if(b._7 >= mtotalViewer){
      mtotalViewer = b._7
    }
    if(b._8 >= monlineViewer){
      monlineViewer = b._8
    }
    (roomId,liveId,wintotalViewer,winonlineViewer,timestamp,date,mtotalViewer,monlineViewer)
  }
}

/**
  * 商品信息增量计算
  * (商品id,主播id,场次id),销售量,最小价,时间戳,日期
  * ((String,String,String),Int,Double,Long,String)
  */
class GoodsAggregateFunction extends AggregateFunction[((String,String,String),Int,Double,Long,String),
  (String,String,String,Int,Double,Long,String,Int,Int),(String,String,String,Int,Double,Long,String,Int)] {
  var map = new util.HashMap[String, String]()

  override def createAccumulator(): (String, String, String, Int, Double, Long, String, Int, Int) = {
    ("", "", "", 0, -1.0, 0L, "", 0, 0)
  }

  override def add(value: ((String, String, String), Int, Double, Long, String), accumulator: (String, String, String, Int, Double, Long, String, Int, Int)): (String, String, String, Int, Double, Long, String, Int, Int) = {

    var first = 0
    var key = value._1._1 + "_" + value._1._2 + "_" + value._1._3
    if (map.get(key) == null) {
      map.put(key, value._2.toString)
      //first = accumulator._4
      first = value._2
      //println("mtotalViewer=" + mtotalViewer + "; monlineViewer="+monlineViewer)
    } else {
      first = map.get(key).toInt
    }

    //println("========= first ==========" + first)

    val promotionId = value._1._1
    val roomId = value._1._2
    val liveId = value._1._3
    var timestamp = accumulator._6
    var salesNumber = accumulator._4 //本窗口最大时间的销量
    var minPrice = accumulator._5 //最低价
    var date = accumulator._7
    if (value._4.toLong > timestamp) {
      timestamp = value._4
      date = value._5
      salesNumber = value._2
    }

    if (minPrice > value._3 || minPrice < 0) {
      minPrice = value._3
    }

    var incr = salesNumber - first

    (promotionId, roomId, liveId, salesNumber, minPrice, timestamp, date, first, incr)
  }

  override def getResult(accumulator: (String, String, String, Int, Double, Long, String, Int, Int)): (String, String, String, Int, Double, Long, String, Int) = {
    map.clear()
    (accumulator._1, accumulator._2, accumulator._3, accumulator._4, accumulator._5, accumulator._6, accumulator._7, accumulator._9)
  }

  override def merge(a: (String, String, String, Int, Double, Long, String, Int, Int), b: (String, String, String, Int, Double, Long, String, Int, Int)): (String, String, String, Int, Double, Long, String, Int, Int) = {
    val promotionId = a._1
    val roomId = a._2
    val liveId = a._3
    var timestamp = a._6
    var salesNumber = a._4
    var minPrice = a._5
    var first = a._8
    if (b._8 <= first) {
      first = b._8
    }

    var date = a._7
    if (b._6.toLong > timestamp) {
      timestamp = b._6
      date = b._7
      salesNumber = b._4
    }
    if (b._5 <= minPrice) { //全窗口最低价
      minPrice = b._5
    }
    var incr = salesNumber - first
    (promotionId, roomId, liveId, salesNumber, minPrice, timestamp, date, first, incr)

  }

}
