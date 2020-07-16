package com.xiaohulu.streaming.sink.essink

import java.net.{MalformedURLException, URL}
import java.util

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/9/5
  * \* Time: 18:28
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
class EsSinkUtil {

  /**
    * es sink
    *
    * @param hosts es hosts
    * @param bulkFlushMaxActions bulk flush size
    * @param parallelism 并行数
    * @param data 数据
    * @param func
    */

  def addSink[T](hosts:java.util.List[HttpHost] , bulkFlushMaxActions:Int,parallelism:Int, data: SingleOutputStreamOperator[T],func: ElasticsearchSinkFunction[T] ):Unit= {
    val esSinkBuilder:ElasticsearchSink.Builder[T] = new ElasticsearchSink.Builder(hosts, func)
    esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions)
    data.addSink(esSinkBuilder.build()).setParallelism(parallelism)
  }

  /**
    * 解析配置文件的 es hosts
    *
    * @param hosts
    * @return
    * @throws MalformedURLException
    */
  def getEsAddresses(hosts:String): util.ArrayList[HttpHost]=   {

    val  hostList = hosts.split(",")
    val addresses = new util.ArrayList[HttpHost]()

    for ( host <- hostList) {
      if (host.startsWith("http")) {
        val url = new URL(host)
        addresses.add(new HttpHost(url.getHost, url.getPort))
      } else {
        val parts = host.split(":", 2)
        if (parts.length > 1) {
          addresses.add(new HttpHost(parts(0), Integer.parseInt(parts(1))))
        } else {
          throw new MalformedURLException("invalid elasticsearch hosts format")
        }
      }
    }
     addresses
  }

}
