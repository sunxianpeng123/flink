package com.xiaohulu.transform.windowFunction

import com.xiaohulu.bean.analysisResultBean.AnchorResultBean
import com.xiaohulu.bean.flinkMapBean.AnchorViewerBean
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/31
  * \* Time: 11:37
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
class AnchorViewerAggregate  extends AggregateFunction[(String, String, Long, Int, Int), AnchorViewerBean, AnchorViewerBean] {
  override def add(in: (String, String, Long, Int, Int), acc: AnchorViewerBean) = {
    acc.platform_id = in._1
    acc.room_id = in._2
    if (in._4 > acc.max_online_viewer) acc.max_online_viewer = in._4
    if (in._5 > acc.max_total_viewer) acc.max_total_viewer = in._5
    acc
  }
  override def createAccumulator() = new AnchorViewerBean

  override def getResult(acc: AnchorViewerBean) = acc

  override def merge(acc: AnchorViewerBean, acc1: AnchorViewerBean) = {
    if (acc.max_online_viewer < acc1.max_online_viewer) acc.max_online_viewer = acc1.max_online_viewer
    if (acc.max_total_viewer < acc1.max_total_viewer) acc.max_total_viewer = acc1.max_total_viewer
    acc
  }
}
