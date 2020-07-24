package com.xiaohulu.tools

import java.text.SimpleDateFormat
import java.util.Date

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/6/11
  * \* Time: 17:40
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
object TimeTool {

  /**
    * 获取当前时间，格式为 2020-06-16 13:34:12
    * @return
    */
  def getNowDatetime(): String = {
    val datetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    datetime
  }

}

