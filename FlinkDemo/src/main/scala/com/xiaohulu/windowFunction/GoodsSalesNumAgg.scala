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
class GoodsSalesNumAgg extends AggregateFunction[GoodsResultBean, GoodsSaleNumBean, GoodsSaleNumBean] with Serializable {
  //  add将每一个元素以某种方式添加到acc中
  override def add(in: GoodsResultBean, acc: GoodsSaleNumBean) = {
    if (acc.timestamp == 0L) {
      acc.room_id = in.room_id
      acc.live_id = in.live_id
      acc.promotion_id = in.promotion_id
      acc.sales_number = in.sales_number
      acc.timestamp = in.timestamp.toLong

    }
    else {
      if (acc.timestamp <= in.timestamp.toLong) acc.sales_number = in.sales_number
    }
    acc
  }

  //  createAccumulator为初始化acc，其目的是用于add第一个元素
  override def createAccumulator() = new GoodsSaleNumBean

  //  getResult获取最终计算结果

  override def getResult(acc: GoodsSaleNumBean) = {
    acc
  }

  //  merge为合并acc;AggregateFunction中的merge方法仅SessionWindow会调用该方法，如果time window是不会调用的，merge方法即使返回null也是可以的。
  override def merge(acc: GoodsSaleNumBean, acc1: GoodsSaleNumBean) = {
    if (acc.timestamp <= acc1.timestamp) acc.sales_number = acc1.sales_number

    acc
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
