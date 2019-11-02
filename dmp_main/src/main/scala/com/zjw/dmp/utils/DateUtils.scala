package com.zjw.dmp.utils

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {

  def getNow(): String = {
    val date = new Date()

    val formatter = FastDateFormat.getInstance("yyyyMMdd")

    val dateStr = formatter.format(date)
    dateStr
  }

  def getYesterday(): String ={
    // 获得当前日期
    val date = new Date()

    // 获取Calendar对象
    val calendar = Calendar.getInstance()
    // 设置Calendar的基准时间
    calendar.setTime(date)
    // 对日期做减法
    calendar.add(Calendar.DAY_OF_YEAR, -1)
    // 日期格式化
    val formatter = FastDateFormat.getInstance("yyyyMMdd")
    val result = formatter.format(calendar)
    result
  }
}
