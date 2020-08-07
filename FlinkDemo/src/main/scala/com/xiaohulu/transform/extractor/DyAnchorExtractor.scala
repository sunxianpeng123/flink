package com.xiaohulu.transform.extractor

import com.xiaohulu.bean.analysisResultBean.AnchorResultBean
import org.apache.flink.api.common.eventtime._

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/27
  * \* Time: 11:45
  * \* To change this template use File | Settings | File Templates.
  * \* Description:  时间戳提取器，水印
  * \*/
class DyAnchorExtractor extends WatermarkStrategy[AnchorResultBean] with Serializable {
  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context) = {
    new WatermarkGenerator[AnchorResultBean] {
      var maxTimestamp: Long = _
      var delay = 3000L

      //onPeriodicEmit : 如果数据量比较大的时候，我们每条数据都生成一个水印的话，会影响性能，所以这里还有一个周期性生成水印的方法。
      // 这个水印的生成周期可以这样设置：env.getConfig().setAutoWatermarkInterval(5000L);
      override def onPeriodicEmit(output: WatermarkOutput) = output.emitWatermark(new Watermark(maxTimestamp - delay))

      //onEvent ：每个元素都会调用这个方法，如果我们想依赖每个元素生成一个水印，然后发射到下游(可选，就是看是否用output来收集水印)，我们可以实现这个方法.
      override def onEvent(event: AnchorResultBean, eventTimestamp: Long, output: WatermarkOutput) = {
        maxTimestamp = Math.max(maxTimestamp, event.timestamp.toLong)
      }
    }
  }
}


