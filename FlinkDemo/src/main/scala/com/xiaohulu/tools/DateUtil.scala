package com.xiaohulu.tools

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalAdjusters}
import java.time._
import java.util.{Calendar, GregorianCalendar}

/**
  * @author xq
  * @version 1.0
  */
object DateUtil extends Serializable {

  val DATE_FORMAT_FULL:String = "yyyy-MM-dd HH:mm:ss"
  val DATE_FORMAT_SHORT:String = "yyyy-MM-dd"
  val DATE_FORMAT_CONNECT:String = "yyyyMMddHHmmss"

  val SDF_FORMAT_FULL = new SimpleDateFormat(DATE_FORMAT_FULL)
  val SDF_FORMAT_SHORT = new SimpleDateFormat(DATE_FORMAT_SHORT)
  val SDF_FORMAT_CONNECT = new SimpleDateFormat(DATE_FORMAT_CONNECT)


  /**
    * 获取当前时间 默认： "yyyy-MM-dd HH:mm:ss"
    * @return
    */
  def getCurDateTimeFull(): String ={
    getCurDateTime(DATE_FORMAT_FULL)
  }

  /**
    * 获取当前时间时间戳
    * @return
    */
  def getNowTimeStamep():Long={
    //Instant.now().toEpochMilli()  // 精确到毫秒
    Instant.now().getEpochSecond // 精确到秒
  }
  /**
    * 获取当前时间
    * @param format 自定义日期格式
    * @return
    */
  def getCurDateTime(format:String):String = {
    val now = LocalDateTime.now
    val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(format)
    val dataTime: String = now.format(dateTimeFormatter)
    dataTime
  }

  /**
    * 长时间字符串 转短点
    * 2020-06-01 转成 20200601 形式
    * @param dateStr
    * @return
    */
  def getStrDateToShortStr(dateStr:String): String ={
    if(dateStr.length!=10){
      throw  new IllegalArgumentException("时间参数长度不是10")
    }else{
      var nStr = dateStr.replaceAll("-","")
      nStr
    }

  }


  /**
    * 计算两个日期相差天数
    * @param start
    * @param end
    * @return
    */
  def getSubDays(start:String, end:String):Int={
    val subDay = ChronoUnit.DAYS.between(LocalDate.parse(start),LocalDate.parse(end))
    subDay.toInt
  }


  /**
    * 得到当前日期的前N天时间
    * @param format
    * @param day
    * @return
    */
  def beforeNDaysDate(format:String, day:Int):String = {
    val dateTimeFormatter = DateTimeFormatter.ofPattern(format)
    val beforeDay = LocalDateTime.now().minusDays(day).format(dateTimeFormatter)
    beforeDay
  }


  /**
    * 获取传入时间的 n天前的时间 默认29天
    * @param endDateDay
    * @param predays
    * @return
    */
  def getPreDay(endDateDay:String,predays:Int=29):String={
    val endTime= SDF_FORMAT_SHORT.parse(endDateDay)
    val calendar=new GregorianCalendar()//Calendar.getInstance()
    calendar.setTime(endTime)
    calendar.add(Calendar.DATE,-1*predays)
    val startDateDay=SDF_FORMAT_SHORT.format(calendar.getTime)
    startDateDay
  }

  /**
    * 日期字符串转时间戳
    * @param dtString 参数类型 "yyyy-MM-dd HH:mm:ss" 格式
    * @return   Long 类型 获取的是毫秒
    **/
  def dateToTimestamp(dtString: String): Long = {
    val dt = SDF_FORMAT_FULL.parse(dtString)
    val timestamp = dt.getTime
    timestamp
  }


  /**
    * 日期字符串转时间戳
    * @param timeStr 参数类型 格式为：yyyy-MM-dd HH:mm:ss
    * @return  Long 类型 毫秒 结果同上 [[dateToTimestamp()]]
    */
  def timeToLong(timeStr: String): Long = {
    this.timeToLong(timeStr,DATE_FORMAT_FULL)
  }

  /**
    * 日期转时间戳
    * @param timeStr
    * @param format
    * @return  Long 类型
    */
  def timeToLong(timeStr: String, format:String): Long = {
    val ftf = DateTimeFormatter.ofPattern(format)
    val parse = LocalDateTime.parse(timeStr, ftf)
    LocalDateTime.from(parse).atZone(ZoneId.systemDefault).toInstant.toEpochMilli
  }


  /**
    * 时间戳转日期字符串
    * @param timestamp  参数是毫秒
    * @return   "yyyyMMddHHmmss"   格式
    **/
  def timestampToDateStr(timestamp: Long): String = {
    val dtString = SDF_FORMAT_CONNECT.format(timestamp)
    dtString
  }


  /**
    * 将时间戳转格式字符串  默认 "yyyy-MM-dd HH:mm:ss"
    * @param timeStamp
    * @return
    */
  def timeToString(timeStamp: Long): String = {
    this.timeToString(timeStamp,DATE_FORMAT_FULL)
  }

  /**
    *
    * @param timeStamp
    * @param format
    * @return
    */
  def timeToString(timeStamp:Long, format:String): String = {
    val ftf = DateTimeFormatter.ofPattern(format)
    ftf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(timeStamp), ZoneId.systemDefault))
  }


  /**
    * 取本月第一天
    */
  def firstDayOfThisMonth():LocalDate={
    val today = LocalDate.now
    today.`with`(TemporalAdjusters.firstDayOfMonth)
  }

  /**
    * 取本月第N天
    */
  def dayOfThisMonth(n: Int): LocalDate = {
    val today = LocalDate.now
    today.withDayOfMonth(n)
  }

  /**
    * 取本月最后一天
    */
  def lastDayOfThisMonth: LocalDate = {
    val today = LocalDate.now
    today.`with`(TemporalAdjusters.lastDayOfMonth)
  }

  /**
    * 取本月第一天的开始时间
    */
  def startOfThisMonth: LocalDateTime = LocalDateTime.of(firstDayOfThisMonth, LocalTime.MIN)


  /**
    * 取本月最后一天的结束时间
    */
  def endOfThisMonth: LocalDateTime = LocalDateTime.of(lastDayOfThisMonth, LocalTime.MAX)



}
