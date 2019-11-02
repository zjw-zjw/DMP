package com.zjw.dmp.tag

import org.apache.spark.sql.Row

/**
  * 生成关键字标签
  */
object KeyWordTag {

  def makeTag(row:Row) ={
    var result = Map[String, Double]()

    val keyWord = row.getAs[String]("keywords")

    // 进行切割:
    keyWord.split(",").map(word => {
      result = result.+((s"KW_${word}", 1))
    })

    result
  }
}
