package com.xiaohulu.streaming.datasource

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/4/4
  * \* Time: 16:16
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
object SocketWindowWCScala {
  def main(args: Array[String]): Unit = {
    val port :Int =try {
      ParameterTool.fromArgs(args).getInt("port")
    }catch {
      case e:Exception =>{
        println("not port set ,give it default port 9000");
      }
        9000
    }
    //nc -l portNum (nc -l 9000)
//  获取运行环境
    val env :StreamExecutionEnvironment =StreamExecutionEnvironment.getExecutionEnvironment
//    链接socket获取输入数据
    val text =env.socketTextStream("masters",port,',')
//  注意：必需要加上这个隐式转换，否则下面的flatmap方法执行会报错
    import org.apache.flink.api.scala._
//    解析数据，吧数据打平，分组，窗口计算，并且聚合求sum
    val windowCounts =text.flatMap(line=>line.split(",")
      .map(w=> WordWithCount(w,1L)) )//把单词转成（word，1）这种形式
      .keyBy("word")
      .timeWindow(Time.seconds(2),Time.seconds(1))//指定窗口大小，指定间隔时间
      .sum("count")//sum或者reduce都可以
//    .reduce((a,b)=>WordWithCount(a.word,a.count+b.count))

    windowCounts.print().setParallelism(1)//打印到控制台
    env.execute("socket window count scala")

  }
  case class WordWithCount(word:String,count:Long)
}

