package com.zjw.dmp.tag

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 生成App应用标签
  */
object AppTag {

  def makeTag(row:Row ,appBC:Broadcast[Map[String, String]] ): Map[String, Double] ={
    var result = Map[String, Double]()

    // 1. 取出广播变量的值
    val appValue = appBC.value
    // 2. 从row中取出appid, appname
    val appId = row.getAs[String]("appid")
    var appName = row.getAs[String]("appname")

    //3、判断，如果appname为空，从广播变量中根据appid取出appname
     appName = Option(appName) match {
      case Some(name) => name
      case None => appValue.getOrElse(appId, "")
    }
    // 4. 生成标签
    result = result.+((s"APP_${appName}", 1))

    // 5. 数据返回
    result
  }
}
