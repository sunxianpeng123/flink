package com.xiaohulu.conf

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.log4j.{Level, Logger}

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/11/11
  * \* Time: 14:22
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
object ParameterTool {


  def getFLinkExecutionEnvironment(): (StreamExecutionEnvironment, StreamTableEnvironment) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //获取flink的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    //
    //    val fsPath = "file:///checkpoints//"
    val fsPath = "file:///F:\\PythonProjects/checkpoints//fs/"
    val rocksdbPath = "file:///F:/checkpoints//rocksdb/"

    //    env.setStateBackend(new MemoryStateBackend())
    //    env.setStateBackend(new FsStateBackend(fsPath, false))
    env.setStateBackend(new RocksDBStateBackend(rocksdbPath, false))
    val config = env.getCheckpointConfig
    //任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
    //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION，表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint处理。
    //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION： 取消作业时保留检查点。请注意，在这种情况下，您必须在取消后手动清理检查点状态。
    //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION： 取消作业时删除检查点。只有在作业失败时，检查点状态才可用。
    //默认情况下，如果设置了Checkpoint选项，则Flink只保留最近成功生成的1个Checkpoint，而当Flink程序失败时，可以从最近的这个Checkpoint来进行恢复。
    // 但是，如果希望保留多个Checkpoint，并能够根据实际需要选择其中一个进行恢复，这样会更加灵活，比如，发现最近4个小时数据记录处理有问题，希望将整个状态还原到4小时之前。
    //Flink可以支持保留多个Checkpoint，需要在Flink的配置文件conf/flink-conf.yaml中，添加如下配置，指定最多需要保存Checkpoint的个数： state.checkpoints.num-retained: 20
    //Flink checkpoint目录分别对应的是 jobId，flink提供了在启动之时通过设置 -s 参数指定checkpoint目录, 让新的jobId 读取该checkpoint元文件信息和状态信息，从而达到指定时间节点启动job。
    config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置checkpoint的周期, 每隔1000 ms进行启动一个检查点
    config.setCheckpointInterval(1000)
    // 设置模式为exactly-once
    config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
    config.setMinPauseBetweenCheckpoints(500)
    // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
    config.setCheckpointTimeout(60000)
    // 同一时间只允许进行一个检查点
    config.setMaxConcurrentCheckpoints(1)
    if (ConfigTool.where_to_run.equals("local")) env.getConfig.setAutoWatermarkInterval(500) else env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    (env, tEnv)

  }


  //        /**
  //          *
  //          * @return
  //          */
  //        def getSparkSession(useHiveSupport:Boolean= false):SparkSession={
  //                Logger.getLogger("org").setLevel(Level.ERROR)
  //                var sqlContext :SparkSession = null
  //                val conf = new SparkConf()
  //                  .setAppName(ConfigTool.spark_app_name)
  //                if  (useHiveSupport) {
  //                        sqlContext= SparkSession
  //                          .builder()
  //                          .config(conf)
  //                          .enableHiveSupport()
  //                          .getOrCreate()
  //                }else{
  //                        val warehouseLocation = "/user/hive/warehouse"
  //                        sqlContext = SparkSession
  //                          .builder()
  //                          .config(conf)
  ////                          .config("hive.metastore.uris", s"thrift://192.168.120.160:9083")
  //                          .config("spark.sql.warehouse.dir", warehouseLocation)
  //                          .getOrCreate()
  //
  //                }
  //                sqlContext
  //        }
  //
  //        /**
  //          * 处理输入参数
  //          * @param args 脚本传递参数
  //          * @param args_num 参数个数
  //          * @param days 一个周期的参数
  //          * @return
  //          */
  //        def getParameters(args:Array[String],args_num:Int,days:Int):(String,String,String,String,Int,String,DateTime)={
  //                //  val properties = new Properties()
  //                //  val path = Thread.currentThread().getContextClassLoader.getResource("db_audienceStatInfo_parquet.properties").getPath //server
  //                //val path = System.getProperty("user.dir") + "/db_audienceStatInfo_parquet.properties" //local
  //                //  val is = new FileInputStream(path)
  //                //  properties.load(is)
  //                if (args.length != args_num) throw new Exception("ERROR，参数个数不正确")
  //                val endDate = args(0)
  //                val runType = args(1).toInt
  //                val platform_id = args(2)
  //                println(s"platform_id = $platform_id")
  //
  //
  //                if (!Set(0,1,9).contains(runType)) throw new Exception("ERROR，runType 设置不正确")
  //                println("runType = 9 run all, runType = 0 run cycle7 ,runType = 1 run cycle 30")
  //
  //                if (endDate.length != 8) throw new Exception("ERROR，日期长度不正确")
  //                val year =endDate.substring(0,4)
  //
  //                var month = endDate.substring(4,6)
  //                if (month.charAt(0).toString=="0") month =month.charAt(1).toString
  //
  //                var day = endDate.substring(6,8)
  //                if (day.charAt(0).toString=="0") day =day.charAt(1).toString
  //                /**time about*/
  //                //todo ,seven date
  //                //    recent week
  //                val rMEDate = new DateTime(year.toInt,month.toInt,day.toInt,19,45,35).toString("yyyy-MM-dd")
  //                val rMBDate = new DateTime(year.toInt,month.toInt,day.toInt,19,45,35).minusDays(days-1).toString("yyyy-MM-dd")
  //                //    last week
  //                val lMEDate = new DateTime(year.toInt,month.toInt,day.toInt,19,45,35).minusDays(days).toString("yyyy-MM-dd")
  //                val lMBDate = new DateTime(year.toInt,month.toInt,day.toInt,19,45,35).minusDays( 2 * days -1).toString("yyyy-MM-dd")
  //                // 主播历史观众日期
  //                val hSnapDate = new DateTime(year.toInt,month.toInt,day.toInt,19,45,35)//.minusDays(days).toString("yyyy-MM-dd")
  //                println(s"days = $days")
  //                println(s"hSnapDate = $hSnapDate")
  //                //todo ,print
  //                println(s"recentWeekEndDate,,recentWeekBeginDate,,lastWeekEndDate,,lastWeekBeginDate,,hSnapDate")
  //                println(rMEDate,rMBDate,lMEDate,lMBDate,runType,platform_id,hSnapDate)
  //                (rMEDate,rMBDate,lMEDate,lMBDate,runType,platform_id,hSnapDate)
  //        }
}

