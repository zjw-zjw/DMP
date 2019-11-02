package com.zjw.dmp.tag

import java.sql.{Connection, Driver, DriverManager, PreparedStatement}

import ch.hsr.geohash.GeoHash
import com.zjw.dmp.utils.{ConfUtils, DateUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 生成商圈库 标签 BA_
  */
object BusinessAreaTag {

  // 指定商圈库表
  val SOURCE_TABLE = s"business_area_${DateUtils.getNow()}"

  /*def makeTag(row: Row, statement: PreparedStatement) = {
    var result = Map[String, Double]()

    // 根据经纬度得到geoHashCode
    val longitude = row.getAs[Float]("longitude")
    val latitude = row.getAs[Float]("latitude")

    if (longitude != null && latitude != null) {
      val geoHashCode: String = GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, 8)
      // 根据geoHashCode 查询 商圈
      statement.setString(1, geoHashCode)
      // 执行查询
      val resultSet = statement.executeQuery()
      // 遍历结果
      while (resultSet.next()) {
        val areas: String = resultSet.getString("areas")
        // 进行切割
        areas.split(",").map(line => {
          result = result.+((s"BA_${line}", 1))
        })
      }
    }
    result
  }*/

  def makeTag(row: Row) = {
    var result = Map[String, Double]()
    //1、取出商圈列表字段
    val areas = row.getAs[String]("areas")
    if (StringUtils.isNotBlank(areas)) {
      // 对商圈进行分割,生成标签
      areas.split(",").map(line => {
        result = result.+((s"BA_${line}", 1))
      })
      result
    } else {

      result
    }
  }
}
