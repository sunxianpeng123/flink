package com.xiaohulu.transform.windowFunction

import com.xiaohulu.bean.GoodsSaleNumTrans
import com.xiaohulu.bean.analysisResultBean.GoodsResultBean
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/28
  * \* Time: 15:28
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
/**
  * 统计货物数据，每个窗口下最大timestamp 的 货物的 sales_number
  */
class GoodsSalesNumAggregate extends AggregateFunction[GoodsResultBean, GoodsSaleNumTrans, GoodsSaleNumTrans] with Serializable {

  override def add(in: GoodsResultBean, acc: GoodsSaleNumTrans) = {
    if (acc.timestamp < in.timestamp.toLong) {
      acc.platform_id = in.platform_id
      acc.room_id = in.room_id
      acc.live_id = in.live_id
      acc.promotion_id = in.promotion_id
      acc.timestamp = in.timestamp.toLong
      acc.sales_number = in.sales_number
    }
    acc
  }

  override def createAccumulator() = GoodsSaleNumTrans("", "", "", "", 0L, 0)

  override def getResult(acc: GoodsSaleNumTrans) = acc

  override def merge(acc: GoodsSaleNumTrans, acc1: GoodsSaleNumTrans) = if (acc.timestamp > acc1.timestamp) acc else acc1
}

