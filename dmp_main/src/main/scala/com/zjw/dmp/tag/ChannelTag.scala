package com.zjw.dmp.tag

import org.apache.spark.sql.Row

object ChannelTag {

  def makeTag(row: Row) ={
    var result = Map[String,Double]()
    //1、取出渠道字段值
    val channel = row.getAs[String]("channelid")
    //2、生成标签
    result = result.+((s"CN_${channel}",1))
    //3、数据返回
    result
  }
}
