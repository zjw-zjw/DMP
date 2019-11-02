package com.zjw.dmp.tag

import org.apache.spark.sql.Row

/**
  * 生成地域标签:省份、城市
  */
object RegionTag {

  def makeTag(row:Row)={
    var result = Map[String, Double]()

    // 省份
    val province = row.getAs[String]("province")

    // 城市
    val city = row.getAs[String]("city")

    result = result.+((s"PV_${province}", 1))
    result = result.+((s"CT_${city}", 1))

    result
  }
}
