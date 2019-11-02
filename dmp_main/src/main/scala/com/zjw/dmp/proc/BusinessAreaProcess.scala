package com.zjw.dmp.proc

import java.util
import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONArray}
import com.zjw.dmp.utils.{ConfUtils, DateUtils, HttpUtils, KuduUtils}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

/**
  * 生成商圈库
  */
object BusinessAreaProcess {

  // 定义原数据读取表
  val SOURCE_TABLE = s"ODS_${DateUtils.getNow()}"
  // 定义数据存入表
  val SINK_TABLE = s"business_area_${DateUtils.getNow()}"

  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession
    val spark = SparkSession.builder().appName("etl").master("local[4]")
      .config("spark.sql.shuffle.partitions", ConfUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.sql.autoBroadcastJoinThreshold", ConfUtils.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD)
      .config("spark.shuffle.compress", ConfUtils.SPARK_SHUFFLE_COMPRESS)
      .config("spark.shuffle.io.maxRetries", ConfUtils.SPARK_SHUFFLE_IO_MAXRETRIES)
      .config("spark.shuffle.io.retryWait", ConfUtils.SPARK_SHUFFLE_IO_RETRYWAIT)
      .config("spark.broadcast.compress", ConfUtils.SPARK_BROADCAST_COMPRESS)
      .config("spark.serializer", ConfUtils.SPARK_SERIALIZER)
      .config("spark.memory.fraction", ConfUtils.SPARK_MEMORY_FRACTION)
      .config("spark.memory.storageFraction", ConfUtils.SPARK_MEMORY_STORAGEFRACTION)
      .config("spark.default.parallelism", ConfUtils.SPARK_DEFAULT_PARALLELISM)
      .config("spark.speculation", ConfUtils.SPARK_SPECULATION)
      .config("spark.speculation.multiplier", ConfUtils.SPARK_SPECULATION_MULTIPLIER)
      .getOrCreate()

    import spark.implicits._
    import org.apache.kudu.spark.kudu._
    // 2. 读取ods层数据
    val source = spark.read
      .option("kudu.master", ConfUtils.MASTER)
      .option("kudu.table", SOURCE_TABLE)
      .kudu

    // 3. 列裁剪, 过滤, 去重
    val filterDS = source.selectExpr("longitude", "latitude")
      .filter("longitude is not null and latitude is not null")
      .distinct()
      .as[(String, String)]

    // 4. 获取商圈
    val result = filterDS.map(item => {
      val longitude = item._1
      val latitude = item._2
      // 将经纬度拼接, 拼接规则: 经度,维度
      val params = s"${longitude},${latitude}"
      // 将拼接后的规则放入url中
      val url = ConfUtils.URL.format(params)
      // 发起http请求,获取数据
      val responseJson: String = HttpUtils.get(url)
      // 解析json,获取经纬度所在商圈列表,如果多个商圈,以逗号分隔
      val areas: String = Try(parseJson(responseJson)).getOrElse("")    // 使用Try处理异常

      // 对经纬度进行geoHash编码，通过编码后，如果经纬度 相近的 那么会生成 同一个编码
      val geoHashCode: String = GeoHash.geoHashStringWithCharacterPrecision(latitude.toDouble, longitude.toDouble, 8)

      // 返回数据
      (geoHashCode, areas)
    })
      .toDF("geoHashCode", "areas")
      .filter("areas is not null and areas != ''")
      .distinct()


    // 5. 结果数据存入kudu
    // 5.1 指定schema信息
    val schema = result.schema
    // 5.2 指定主键字段
    val keys = Seq("geoHashCode")
    val kuduContext = new KuduContext(ConfUtils.MASTER, spark.sparkContext)

    KuduUtils.write(SINK_TABLE, schema, result, keys, kuduContext)

  }


  /**
    * 解析Json字符串, 商圈name1, 商圈name2
    * @param jsonStr
    */
  def parseJson(jsonStr:String) = {
    val jSONObject = JSON.parseObject(jsonStr)

    val regeocode = jSONObject.getJSONObject("regeocode")

    val addressComponent = regeocode.getJSONObject("addressComponent")

    val businessAreas: JSONArray = addressComponent.getJSONArray("businessAreas")

    val areas: util.List[BusinessArea] = businessAreas.toJavaList(classOf[BusinessArea])

    import scala.collection.JavaConversions._
      
    // 将数组形式转换为 name1,name2,name3...
    areas.map(_.name).mkString(",")
  }
}

case class BusinessArea(id:String, location:String, name:String)
