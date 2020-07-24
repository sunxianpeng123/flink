package com.xiaohulu.mysqlsink.bean

import scala.collection.mutable.ListBuffer

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2019/7/8
  * \* Time: 16:58
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
//@JsonIgnoreProperties(ignoreUnknown = true)
class WWWBeanList extends  Serializable {
  var buffer: ListBuffer[WWWBean] = ListBuffer()


}
