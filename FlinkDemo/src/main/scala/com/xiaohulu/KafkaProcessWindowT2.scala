//package com.xiaohulu
//
//import org.apache.flink.api.common.functions.ReduceFunction
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.types.Row
//import org.apache.flink.api.common.functions.AggregateFunction
//
//import scala.collection.mutable.ListBuffer
//import com.google.gson.{Gson, JsonParser}
//import java.util.Properties
//
//import bean._
//import com.xiaohulu.control.handler
//import com.xiaohulu.tools.PropertyUtil
//import common.{PropertyUtil, handler}
//import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
//import org.apache.flink.streaming.api.environment.CheckpointConfig
//import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
//import transform.{KafkaSourceTransform, MyKafProcessWindowFunction2, MyPeriodicWatermark}
//import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
//import org.apache.flink.streaming.api.watermark.Watermark
//import org.apache.flink.table.api._
//import org.apache.flink.table.api.bridge.scala._
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.api.scala.StreamTableEnvironment
//import org.apache.flink.util.Collector
//
//import util.DateUtil
//
////java 转 scala
//import scala.collection.JavaConversions._
//
///**
//  * @author xq
//  * @date 2020/7/29 9:42
//  * @version 1.0
//  * @describe:
//  */
//
//object KafkaProcessWindowT2 {
//
//  handler.propertyUtilInit()
//
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//    // 1分钟 启动一次checkpoint 根据数据容忍度
//    env.enableCheckpointing(60000)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) //精确一致
//    // 设置两次checkpoint的最小时间间隔 5秒
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)
//    // 允许的最大checkpoint并行度
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
//    // checkpoint超时的时长 1分钟
//    env.getCheckpointConfig.setCheckpointTimeout(60000)
//    // 当程序关闭的时，触发额外的checkpoint
//    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//
//    val path = "file:///E:/work/xq/flink/checkpoint/one"
//    val rocksBackend = new RocksDBStateBackend(path, true)
//    env.setStateBackend(rocksBackend)
//
//
//    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
//    val tableEnv = StreamTableEnvironment.create(env, bsSettings)
//
//
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", PropertyUtil.getString("brokers"))
//    properties.setProperty("group.id", "test22")
//
//    /*val topic = PropertyUtil.getString("anchor_goods_topics")
//    val kafkaSource = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)*/
//
//    val topics = new ListBuffer[String]
//    val t1 = PropertyUtil.getString("anchor_basic_info_topics")
//    val t2 = PropertyUtil.getString("anchor_goods_topics")
//    topics.+=(t1)
//    topics.+=(t2)
//    //val kafkaSource = new FlinkKafkaConsumer[String](topics, new SimpleStringSchema(), properties)
//    val kafkaSource = new FlinkKafkaConsumer[String](topics, new SimpleStringSchema(), properties)
//
//    //指定kafka的消费 位点
//    //kafkaSource.setStartFromEarliest()
//    kafkaSource.setStartFromLatest()
//
//    val input = env.addSource(kafkaSource).uid("s1")
//
//    val arrStream = KafkaSourceTransform.sourceTrans(input)
//    val packageObj = KafkaSourceTransform.dataSourceToArrObj(arrStream)
//
//    val anchor:DataStream[AnchorResultBean] = KafkaSourceTransform.tObjGetOneAnchor(packageObj).filter(_!=null).uid("s7")
//    val goods:DataStream[GoodsResultBean] = KafkaSourceTransform.tObjGetOneGoods(packageObj).filter(_!=null).uid("s10")
//
//    val resStreamGoods = KafkaSourceTransform.getGoodsIncrInfo(goods)
//    val resStreamAnchor = KafkaSourceTransform.getAnchorInfo(anchor)
//
//
//    // 第一种方式 ： 使用流join 查询
//    //resStreamGoods.coGroup(resStreamAnchor)
//      /*.where(<key selector>)//where条件:左流的Key
//        .equalTo(<key selector>)//equalTo条件:右流的Key
//          .window(TumblingEventTimeWindows.of(Time.seconds(3)))//开启Window窗口
//          .apply (new CoGroupFunction () {...});//处理逻辑:JoinFunction 或 FlatJoinFunction*/
//
//    // 第二种方式 ： 把 DataStream 转成 Table
//    //商品数据
//    tableEnv.createTemporaryView("goods", resStreamGoods,'room_id,'live_id, 'time_minut,
//      'promotion_id, 'sales_number,'min_price, 'date, 'incr_sales, 'start_time)
//    //主播数据
//    tableEnv.createTemporaryView("anchor", resStreamAnchor,'room_id,'live_id, 'time_minut,
//      'totalviewer, 'onlineviewer,'timestamp, 'date)
//
//    val joinSql = """
//                     |select g.room_id,g.live_id,g.time_minut,g.promotion_id,g.sales_number,g.min_price,
//                     | g.incr_sales,g.start_time,a.totalviewer,a.onlineviewer,a.`timestamp`,a.`date`
//                     | from goods g
//                     | full outer join anchor a
//                     | on g.room_id = a.room_id and g.live_id=a.live_id and g.time_minut=a.time_minut
//                     |
//                   """.stripMargin
//
//    val sql = """
//                |select g.min_price,g.incr_sales,a.totalviewer,a.onlineviewer,g.start_time,a.`timestamp`
//                | from goods g
//                | full outer join anchor a
//                | on g.room_id = a.room_id and g.live_id=a.live_id and g.time_minut=a.time_minut
//                |
//              """.stripMargin
//
//    //val tableJoin = tableEnv.sqlQuery(joinSql)
//    val tableJoin = tableEnv.sqlQuery(sql)
//
//    tableJoin.toRetractStream[Row].print()
//
//    //resStreamGoods.print()
//    //resStreamAnchor.print()
//
//    env.execute("kafka")
//
//  }
//
//}
//
