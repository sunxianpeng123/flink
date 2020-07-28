package com.xiaohulu.windowFunction

import com.xiaohulu.bean.{GoodsResultBean, GoodsSaleNumBean}
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/28
  * \* Time: 15:28
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
class GoodsSalesNumAgg extends AggregateFunction[GoodsResultBean, (String, String, String, Long, Long), (String, String, String, Long, Long)] {
  //  add将每一个元素以某种方式添加到acc中
  override def add(in: GoodsResultBean, acc: (String, String, String, Long, Long)) = {
    var res: (String, String, String, Long, Long) = (null, null, null, 0L, 0L)
    if (acc._5 == 0L) {
      res = (in.room_id, in.live_id, in.promotion_id, in.sales_number, in.timestamp.toLong)
    } else {
      if (acc._5 <= in.timestamp.toLong) res = (in.room_id, in.live_id, in.promotion_id, in.sales_number, in.timestamp.toLong)
      else res = acc
    }
    res
  }

  //  createAccumulator为初始化acc，其目的是用于add第一个元素
  override def createAccumulator() = {
//    println("我是累加器createAccumulator===========")
    (null, null, null, 0L, 0L)
  }

  //  getResult获取最终计算结果

  override def getResult(acc: (String, String, String, Long, Long)) = {
    println("我是getResult===========")
    acc
  }

  //  merge为合并acc;并行执行时，合并每个结果
  override def merge(acc: (String, String, String, Long, Long), acc1: (String, String, String, Long, Long)) = {
    var res: (String, String, String, Long, Long) = (null, null, null, 0L, 0L)
    if (acc._5 <= acc1._5) res = (acc._1, acc._2, acc._3, acc1._4, acc._5)

    res
  }
}

/*class MyAggregateFunction extends AggregateFunction[GoodsResultBean,Int,GoodsResultBean]{
  override def add(in: GoodsResultBean, ac: Int) = {
    ac + in.sales_number
    //in
  }
  override def createAccumulator() = 0
  override def getResult(ac: Int) = {
    val acc = new GoodsResultBean
    acc.sales_number= ac
    acc
  }
  override def merge(acc: Int, acc1: Int) = {
    acc+acc1
  }
}*/
