package com.xiaohulu.bean.flinkMapBean

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/7/31
  * \* Time: 11:27
  * \* To change this template use File | Settings | File Templates.
  * \* Description: 
  * \*/
class AnchorViewerBean extends Serializable {
  var platform_id: String = _
  var room_id: String = _
  var max_online_viewer: Long = _
  var max_total_viewer: Long = _

  override def toString = s"AnchorViewerBean(platform_id=$platform_id, room_id=$room_id, max_online_viewer=$max_online_viewer, max_total_viewer=$max_total_viewer)"
}
