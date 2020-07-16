package com.xiaohulu.streaming.sink.essink

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/9/5
  * \* Time: 18:41
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object EsSinkApp {
  def main(args: Array[String]): Unit = {
//    kafka.brokers=localhost:9092
//    kafka.group.id=zhisheng-metrics-group-test
//    kafka.zookeeper.connect=localhost:2181
//    metrics.topic=zhisheng-metrics
//    stream.parallelism=5
//    stream.checkpoint.interval=1000
//    stream.checkpoint.enable=false
//    elasticsearch.hosts=localhost:9200
//    elasticsearch.bulk.flush.max.actions=40
//    stream.sink.parallelism=5
//    1、bulk.flush.backoff.enable 用来表示是否开启重试机制
//
//    2、bulk.flush.backoff.type 重试策略，有两种：EXPONENTIAL 指数型（表示多次重试之间的时间间隔按照指数方式进行增长）、CONSTANT 常数型（表示多次重试之间的时间间隔为固定常数）
//
//    3、bulk.flush.backoff.delay 进行重试的时间间隔
//
//    4、bulk.flush.backoff.retries 失败重试的次数
//
//    5、bulk.flush.max.actions: 批量写入时的最大写入条数
//
//    6、bulk.flush.max.size.mb: 批量写入时的最大数据量
//
//    7、bulk.flush.interval.ms: 批量写入的时间间隔，配置后则会按照该时间间隔严格执行，无视上面的两个批量写入配置

//    //获取所有参数
//
//    val  parameterTool = ExecutionEnvUtil.createParameterTool(args)
//    //准备好环境
//    val env = ExecutionEnvUtil.prepare(parameterTool)
//    //从kafka读取数据
//    val  data = KafkaConfigUtil.buildSource(env)
//
//    //从配置文件中读取 es 的地址
//    val esSinkUtil = new EsSinkUtil
//    val esAddresses = esSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS))
//    //从配置文件中读取 bulk flush size，代表一次批处理的数量，这个可是性能调优参数，特别提醒
//    val bulkSize = parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40)
//    //从配置文件中读取并行 sink 数，这个也是性能调优参数，特别提醒，这样才能够更快的消费，防止 kafka 数据堆积
//    val sinkParallelism = parameterTool.getInt(STREAM_SINK_PARALLELISM, 5)
//    //自己再自带的 es sink 上一层封装了下
//    esSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, data,
//      (Metrics metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
//        requestIndexer.add(Requests.indexRequest()
//          .index(ZHISHENG + "_" + metric.getName())  //es 索引名
//          .type(ZHISHENG) //es type
//          .source(GsonUtil.toJSONBytes(metric), XContentType.JSON));
//      })
//    env.execute("flink learning connectors es6")

  }
}

