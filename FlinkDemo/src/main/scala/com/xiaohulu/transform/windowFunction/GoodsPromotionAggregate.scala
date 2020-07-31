package com.xiaohulu.transform.windowFunction

import com.xiaohulu.bean.analysisResultBean.GoodsResultBean
import com.xiaohulu.bean.flinkMapBean.{GoodsPromotionBean, GoodsSaleNumBean}
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/28
  * \* Time: 15:28
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
case class AccumulatorBean(platform_id: String, promotion_id: String, max_seckill_min_price: Double, min_min_price: Double, max_coupon: Double, max_promote_remark: String)

//case class ResultBean(platform_id: String, promotion_id: String, mp: Double)

class GoodsPromotionAggregate extends AggregateFunction[GoodsResultBean, AccumulatorBean, GoodsPromotionBean] {
  override def add(in: GoodsResultBean, acc: AccumulatorBean) = {
    var max_seckill_min_price = 0.0
    var min_min_price = 0.0
    var max_coupon = 0.0
    var max_promote_remark = ""
    if (in.seckill_min_price > acc.max_seckill_min_price) max_seckill_min_price = in.seckill_min_price
    if (in.min_price < acc.min_min_price) min_min_price = in.min_price
    if (in.coupon > acc.max_coupon) max_coupon = in.coupon
    if (in.promote_remark > acc.max_promote_remark) max_promote_remark = in.promote_remark


    AccumulatorBean(in.platform_id, in.promotion_id, max_seckill_min_price, min_min_price, max_coupon, max_promote_remark)
  }

  override def createAccumulator() = AccumulatorBean("", "", 0.0, 0.0, 0.0, "")

  override def getResult(acc: AccumulatorBean) = {
    //查看满减策略是否符合标准
    val standardArray = regexStandard(acc.max_promote_remark)
    val standard = standardArray(0) * 100.0
    val discounts = standardArray(1) * 100.0
    //满x元，省y元
    var mp = acc.min_min_price
    if (acc.max_seckill_min_price > 0) mp = acc.max_seckill_min_price
    if (mp > standard) mp = mp - discounts
    //优惠券
    if (mp > acc.max_coupon) mp = mp - acc.max_coupon
//    汇总结果
    val goodsPromotionBean = new GoodsPromotionBean
    goodsPromotionBean.platform_id = acc.platform_id
    goodsPromotionBean.promotion_id = acc.promotion_id
    goodsPromotionBean.mp = mp
    goodsPromotionBean
  }

  override def merge(acc: AccumulatorBean, acc1: AccumulatorBean) = {
    var max_seckill_min_price = 0.0
    var min_min_price = 0.0
    var max_coupon = 0.0
    var max_promote_remark = ""
    if (acc.max_seckill_min_price > acc1.max_seckill_min_price) max_seckill_min_price = acc.max_seckill_min_price
    if (acc.min_min_price < acc1.min_min_price) min_min_price = acc.min_min_price
    if (acc.max_coupon > acc1.max_coupon) max_coupon = acc.max_coupon
    if (acc.max_promote_remark > acc1.max_promote_remark) max_promote_remark = acc.max_promote_remark
    AccumulatorBean(acc.platform_id, acc.promotion_id, max_seckill_min_price, min_min_price, max_coupon, max_promote_remark)
  }

  def regexStandard(promote_remark_max: String): Array[Double] = {
    var arr: Array[Double] = Array.empty
    val pattern = "[\\d]+\\.*[\\d]*".r
    val group = pattern.findAllIn(promote_remark_max)
    while (group.hasNext) arr :+= group.next().toDouble
    if (arr.length < 2) arr = Array(0.0, 0.0)
    arr
  }
}
