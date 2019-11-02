package com.zjw.dmp.tag

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 生成设备标签:设备[DV_设备型号<htc、iphone>, DT_设备类型<1：android 2：ios 3：wp>,
  * ISP_运营商名称, NW_联网方式名称, DP_设备类型<手机,平板>]
  */
object DeviceTag {

  def makeTag(row:Row, deviceBC:Broadcast[Map[String, String]]) ={
    var result = Map[String, Double]()

    // 1. 取出广播变量值
    val deviceValue = deviceBC.value
    // 2. 取出设备型号、设备类型、运营商、联网方式、设备类型
    val device = row.getAs[String]("device")

    val client = row.getAs[Long]("client")

    val ispName = row.getAs[String]("ispname")

    val netWork = row.getAs[String]("networkmannername")

    val deviceType = row.getAs[Long]("devicetype")

    // 3. 将设备类型、联网方式、运营商转换为企业内部编码
    val clientCode = deviceValue.getOrElse(client.toString, "OTHER")

    val ispNameCode = deviceValue.getOrElse(ispName, "OTHER")

    val networkCode = deviceValue.getOrElse(netWork, "OTHER")

    val deviceTypeName = if (deviceType == 1L) "手机" else "平板"
    /*
        ispName: 联通	network: NETWORKOTHER
    ispName: 电信	network: NETWORKOTHER
    ispName: 移动	network: NETWORKOTHER
     */
    // 4. 生成标签
    result = result.+((s"DV_${device}", 1))
    result = result.+((s"DT_${clientCode}", 1))
    result = result.+((s"ISP_${ispNameCode}", 1))
    result = result.+((s"NW_${networkCode}", 1))
    result = result.+((s"NW_${deviceTypeName}", 1))
    // 5. 数据返回
   // println("ispName: " + ispName + "\t" + "network: " + netWork + "client:" + client)
    result
  }
}
