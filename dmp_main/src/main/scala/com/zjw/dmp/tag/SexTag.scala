package com.zjw.dmp.tag

import org.apache.spark.sql.Row

/**
  * 生成性别标签
  */
object SexTag {

  def makeTag(row:Row) ={
    var result = Map[String, Double]()

    val sex = if(row.getAs[String]("sex") == 0) "女" else "男"

    result = result.+((s"SEX_${sex}", 1))

    result
  }
}
