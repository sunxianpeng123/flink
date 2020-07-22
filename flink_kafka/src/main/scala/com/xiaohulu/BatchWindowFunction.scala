package com.xiaohulu

import com.xiaohulu.bean.{WWWBean, WWWBeanList}
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class BatchWindowFunction extends ProcessAllWindowFunction[WWWBean, WWWBeanList, TimeWindow] {

  override def process(context: Context, elements: Iterable[WWWBean], out: Collector[WWWBeanList]): Unit = {
    var buffer: ListBuffer[WWWBean] = ListBuffer()
    val iterator = elements.iterator
    while (iterator.hasNext) {
      buffer.append(iterator.next())
    }
    var wwwList = new WWWBeanList
    wwwList.buffer=buffer
    out.collect(wwwList)
  }

}