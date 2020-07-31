package com.xiaohulu.transform.windowFunction

import com.xiaohulu.bean.analysisResultBean.GoodsResultBean
import com.xiaohulu.bean.flinkMapBean.GoodsSaleNumBean
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/28
  * \* Time: 15:28
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
//case class GoodsSaleNumBean( platform_id :String , room_id: String , promotion_id: String , live_id: String , timestamp: Long , sales_number: Long )

class GoodsSalesNumAggregate extends AggregateFunction[GoodsResultBean, GoodsSaleNumBean, GoodsSaleNumBean] with Serializable {
  override def add(in: GoodsResultBean, acc: GoodsSaleNumBean) = {
    if (acc.timestamp < in.timestamp.toLong) {
      acc.room_id = in.room_id
      acc.live_id = in.live_id
      acc.promotion_id = in.promotion_id
      acc.sales_number = in.sales_number
      acc.timestamp = in.timestamp.toLong
    }
    acc
  }

  override def createAccumulator() = new GoodsSaleNumBean

  override def getResult(acc: GoodsSaleNumBean) = acc

  override def merge(acc: GoodsSaleNumBean, acc1: GoodsSaleNumBean) = if (acc.timestamp > acc1.timestamp) acc else acc1
}

