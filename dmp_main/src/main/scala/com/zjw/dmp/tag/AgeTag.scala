package com.zjw.dmp.tag

import org.apache.spark.sql.Row

/**
  * 生成年龄标签
  */
object AgeTag {

  def makeTag(row:Row)={
    var result = Map[String, Double]()

    val age = row.getAs[String]("age")

    result = result.+((s"AGE_${age}", 1))

    result
  }
}
