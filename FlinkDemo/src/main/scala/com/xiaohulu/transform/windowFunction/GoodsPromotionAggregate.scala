package com.xiaohulu.transform.windowFunction

import com.xiaohulu.bean.{GoodsPromotionAggTrans, GoodsPromotionTrans}
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
  * 统计窗口下，货物的 如下字段的 最大值或者最小值
  */
class GoodsPromotionAggregate extends AggregateFunction[GoodsResultBean, GoodsPromotionAggTrans, GoodsPromotionTrans] {
  override def add(in: GoodsResultBean, acc: GoodsPromotionAggTrans) = {
    var max_seckill_min_price = 0.0
    var min_min_price = 0.0
    var max_coupon = 0.0
    var max_promote_remark = ""
    if (in.seckill_min_price > acc.max_seckill_min_price) max_seckill_min_price = in.seckill_min_price
    if (in.min_price < acc.min_min_price) min_min_price = in.min_price
    if (in.coupon > acc.max_coupon) max_coupon = in.coupon
    if (in.promote_remark > acc.max_promote_remark) max_promote_remark = in.promote_remark

    GoodsPromotionAggTrans(in.platform_id, in.promotion_id, max_seckill_min_price, min_min_price, max_coupon, max_promote_remark)
  }

  override def createAccumulator() = GoodsPromotionAggTrans("", "", 0.0, 0.0, 0.0, "")

  override def getResult(acc: GoodsPromotionAggTrans) = {
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
//    汇总结果GoodsPromotionTrans
    GoodsPromotionTrans(acc.platform_id,acc.promotion_id,mp)
  }

  override def merge(acc: GoodsPromotionAggTrans, acc1: GoodsPromotionAggTrans) = {
    var max_seckill_min_price = 0.0
    var min_min_price = 0.0
    var max_coupon = 0.0
    var max_promote_remark = ""
    if (acc.max_seckill_min_price > acc1.max_seckill_min_price) max_seckill_min_price = acc.max_seckill_min_price
    if (acc.min_min_price < acc1.min_min_price) min_min_price = acc.min_min_price
    if (acc.max_coupon > acc1.max_coupon) max_coupon = acc.max_coupon
    if (acc.max_promote_remark > acc1.max_promote_remark) max_promote_remark = acc.max_promote_remark
    GoodsPromotionAggTrans(acc.platform_id, acc.promotion_id, max_seckill_min_price, min_min_price, max_coupon, max_promote_remark)
  }

  /**
    * 正则表达式匹配满减优惠策略
    * @param promote_remark_max
    * @return
    */
  def regexStandard(promote_remark_max: String): Array[Double] = {
    var arr: Array[Double] = Array.empty
    val pattern = "[\\d]+\\.*[\\d]*".r
    val group = pattern.findAllIn(promote_remark_max)
    while (group.hasNext) arr :+= group.next().toDouble
    if (arr.length < 2) arr = Array(0.0, 0.0)
    arr
  }
}
