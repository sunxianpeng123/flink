package com.xiaohulu.tools

import java.text.{ParseException, SimpleDateFormat}
import java.time.{YearMonth, ZoneId}
import java.util.{Calendar, Date, GregorianCalendar}

import org.joda.time.DateTime

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2017/10/24
  * \* Time: 15:38
  * \* To change this template use File | Settings | File Templates.
  * \* Description:
  * \*/
object Helper {
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  val format1 = new SimpleDateFormat("yyyyMMdd")
  val format2 = new SimpleDateFormat("yyyy-MM-dd")


  def filterEmoji(source: String): String = {
    if (source != null && source.length() > 0) {
      source.replaceAll("[\ud800\udc00-\udbff\udfff\ud800-\udfff]", "")//过滤Emoji表情
        .replaceAll("[\u2764\ufe0f]","")//过滤心形符号
    } else {
      source
    }
  }

  def transformDate(endDateDay:String):String={
    //20170718--->2017-07-18
    val endDate=new Date(format1.parse(endDateDay).getTime)
    val end=format2.format(endDate)
    end
  }

  def getDateDayOfWeek(endDateDay:String):Int={
    var res:Int=0
    try {
      val date1 = new Date(format2.parse(endDateDay).getTime)
      val calendar = Calendar.getInstance()
      calendar.setTime(date1)
      res=calendar.get(Calendar.DAY_OF_WEEK)-1
    } catch {
      case  e:ParseException=>e.printStackTrace
    }
    res
  }

  def getAdEndDate(endDateDay:String):String={
    var res =""
    val t1=format2.parse(transformDate(endDateDay))
    val cal =Calendar.getInstance()
    cal.setTime(t1)
    val dayOfWeek=getDateDayOfWeek(transformDate(endDateDay))
    cal.add(Calendar.DATE,-1 * dayOfWeek)
    res=format2.format(cal.getTime)
    res
  }

  def getAdStartDate(adEndDate:String):String={
    var res =""
    val t1=format2.parse(adEndDate)
    val cal =Calendar.getInstance()
    cal.setTime(t1)
    cal.add(Calendar.DATE,-1 * 6)
    res=format2.format(cal.getTime)
    res
  }

  /**
    *
    * @param endDateDay 格式 20190611
    * @param predays
    * @return  格式 2019-05-13
    */
  def getPreDay(endDateDay:String,predays:Int=29):String={
    //将endDateDay向前推predays天
    val endTime= format2.parse(transformDate(endDateDay))
    val calendar=new GregorianCalendar()//Calendar.getInstance()
    calendar.setTime(endTime)
    calendar.add(Calendar.DATE,-1*predays)
    val startDateDay=format2.format(calendar.getTime)
    startDateDay
  }

  def getDateWeek(endDateDay:String):Int={
    var res:Int=0
    try {
      val time =format2.parse(endDateDay)//.getTime
//      val date2=format2.format(date1)
      val calendar = Calendar.getInstance()

      calendar.setFirstDayOfWeek(Calendar.MONDAY)
      calendar.setTime(time)
     res=calendar.get(Calendar.WEEK_OF_YEAR)
    } catch {
      case  e:ParseException=>e.printStackTrace()
    }
    res
  }

  /**
    *
    * @param typeName 开始 or 结束
    */
  def getAndOutputTaskStartTimeMillis(typeName:String):Long={
    var timeMillis:Long =0L
    val set =Set("start","end","开始","结束")
    if (set.contains(typeName)){
      timeMillis = DateTime.now().getMillis
//      println(s"******************任务${typeName}时间:$timeMillis******************")
    }else{
     throw  new IllegalArgumentException(typeName +"is not in ( " +set.mkString(",") + " ) ")
    }
    timeMillis
  }
  /**
    * 2019-08-01 00:00:00格式转换为1564588800格式
    * @param datetimeStr
    * @return
    */
  def  datetimeToStamp(datetimeStr:String):Long={
    val  simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val  date = simpleDateFormat.parse(datetimeStr);
    val  res= date.getTime/1000;
    res
  }


  /**
    *1564588800格式转换为2019-08-01 00:00:00 格式
    * @param timestampLong
    * @return
    */
  def  timestampToDatetime(timestampLong:Long):String={

    val  date = new Date(timestampLong*1000);
    val res = sdf.format(date);
    res
  }

  /**
    * 获取指定年月的开始时间和结束时间
    * @param year
    * @param month
    * @return
    */
  def  getMonthBeginTime( year:Int, month:Int) :String={
    val yearMonth = YearMonth.of(year, month)
    val  localDate = yearMonth.atDay(1)
    val startOfDay = localDate.atStartOfDay()
    val zonedDateTime = startOfDay.atZone(ZoneId.of("Asia/Shanghai"))
    sdf.format(Date.from(zonedDateTime.toInstant))
  }
  def getMonthEndTime(year:Int, month:Int):String={
    val yearMonth = YearMonth.of(year, month)
    val endOfMonth = yearMonth.atEndOfMonth();
    val localDateTime = endOfMonth.atTime(23, 59, 59, 999);
    val zonedDateTime = localDateTime.atZone(ZoneId.of("Asia/Shanghai"))
    sdf.format(Date.from(zonedDateTime.toInstant))
  }


}
