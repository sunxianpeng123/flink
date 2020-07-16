package com.xiaohulu.streaming.windowed.timewindow

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/4/4
  * \* Time: 16:16
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
object FLinkTransWindow {
  def main(args: Array[String]): Unit = {
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        println("not port set ,give it default port 9000");
      }
        9000
    }
    //nc -l portNum (nc -l 9000)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("masters", port, ',')
    import org.apache.flink.api.scala._

    val streamKeyBy = stream.map(item => (item, 1L))
      .keyBy(0)
    val streamWindow = streamKeyBy
//      .countWindow(5) //窗口大小5个单词,当同一个key达到5次时才会输出，当不同key但是达到五次不会输出
      .countWindow(5,2) //窗口大小5个单词,当同一个key每达到2次时会输出一次，并且当同一个key出现5次时也会执行一次，

      .reduce((x, y) => (x._1, x._2 + y._2))
    streamWindow.print()
    env.execute("socket window count scala")

  }
  case class WordWithCount(word: String, count: Long)
}

