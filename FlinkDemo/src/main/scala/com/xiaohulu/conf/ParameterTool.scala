//package com.xiaohulu.conf
//
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//import org.joda.time.DateTime
//
///**
//  * \* Created with IntelliJ IDEA.
//  * \* User: sunxianpeng
//  * \* Date: 2019/11/11
//  * \* Time: 14:22
//  * \* To change this template use File | Settings | File Templates.
//  * \* Description:
//  * \*/
//object ParameterTool {
//
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
//}
//
