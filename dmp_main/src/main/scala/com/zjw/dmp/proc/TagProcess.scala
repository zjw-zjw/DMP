package com.zjw.dmp.proc

import java.sql.{Connection, DriverManager, PreparedStatement}

import ch.hsr.geohash.GeoHash
import com.zjw.dmp.agg.TagAgg
import com.zjw.dmp.graphx.UserGraphx
import com.zjw.dmp.history.HistoryTag
import com.zjw.dmp.tag._
import com.zjw.dmp.utils.{ConfUtils, DateUtils, KuduUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import sun.misc.ObjectInputFilter.Config

import scala.tools.scalap.scalax.util.StringUtil

/**
  * 生成用户的标
  */
object TagProcess {

  // 指定原表
  val SOURCE_TABLE = s"ODS_${DateUtils.getNow()}"
  // 指定商圈表
  val BUSINESS_AREA_TABLE = s"business_area_${DateUtils.getNow()}"
  // 指定标签表
  val TAG_SINK_TABLE = s"tag_${DateUtils.getYesterday()}"
  // 指定聚合标签表
  val TAG_TABLE = s"tag_${DateUtils.getNow()}"

  def main(args: Array[String]): Unit = {
    // 1. 创建SparkSession
    val spark = SparkSession.builder()
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
      .appName("tag").master("local[4]").getOrCreate()

    import org.apache.kudu.spark.kudu._
    // 2. 读取ODS层数据
    val source = spark.read
      .option("kudu.master", ConfUtils.MASTER)
      .option("kudu.table", SOURCE_TABLE)
      .kudu

    // 3. 列裁剪, 过滤, 去重
    val filteredDF = source.filter(
      """
        |(imei is not null and imei != '') or
        |(mac is not null and mac != '') or
        |(idfa is not null and idfa != '') or
        |(openudid is not null and openudid != '') or
        |(androidid is not null and androidid != '')
      """.stripMargin)

    filteredDF.createOrReplaceTempView("source")

    // 4. 读取商圈表, 并创建临时表, 为后面获得商圈库标签作准备
    spark.read
      .option("kudu.master", ConfUtils.MASTER)
      .option("kudu.table", BUSINESS_AREA_TABLE)
      .kudu
      .createOrReplaceTempView("business")

    // 5. 注册UDF函数
    spark.udf.register("geoGeoHashCode", getGeoHashCode _)

    // 广播business表  这一步很重要,不然速度就很慢
    spark.sql("cache table business")

    val joinDF: DataFrame = spark.sql(
      """
        |select s.*, b.areas
        | from source s left join business b
        |  on geoGeoHashCode(s.longitude, s.latitude) = b.geoHashCode
      """.stripMargin)

    // 6. 读取APP字典文件
    import spark.implicits._

    val appSource: Dataset[String] = spark.read.textFile(ConfUtils.APPID_NAME)
    val appDS: Dataset[(String, String)] = appSource.map(line => {
      val arr = line.split("##")
      val appId = arr.head
      val appName = arr.last
      (appId, appName)
    })
    val appMap = appDS.collect().toMap

    // 7. 读取设备的字典文件
    val deviceSource = spark.read.textFile(ConfUtils.DEVICEDIC)
    val deviceDS = deviceSource.map(line => {
      val arr = line.split("##")
      val value = arr.head
      val code = arr.last
      (value, code)
    })
    val deviceMap = deviceDS.collect().toMap
    // 8. 广播字典文件数据
    val appBC: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(appMap)
    val deviceBC: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(deviceMap)

    // 9. 生成标签
    // 设备[APP_应用名称, DV_设备型号<htc、iphone>, DT_设备类型, ISP_运营商名称, NW_联网方式名称,DP_设备类型<手机,平板>],
    // PV_省份, CT_城市, , KW_关键字,  用户[AGE_年龄, SEX_性别]
    // BA_商圈
    val tagRDD: RDD[(String, (List[String], Map[String, Double]))] = joinDF.rdd.map(row=> {

      // 4.1 生成APP标签
      val appTags = AppTag.makeTag(row, appBC)

      // 4.2 生成设备标签
      val deviceTags = DeviceTag.makeTag(row, deviceBC)

      // 4.3 生成地域标签
      val regionTags = RegionTag.makeTag(row)

      // 4.4 生成渠道标签
      val channelTags = ChannelTag.makeTag(row)

      // 4.5 生成关键字标签
      val keyWordsTags = KeyWordTag.makeTag(row)

      // 4.6 生成年龄标签
      val ageTags = AgeTag.makeTag(row)

      // 4.7 生成性别标签
      val sexTags = SexTag.makeTag(row)

      // 4.8 生成商圈标签
      val businessAreaTags = BusinessAreaTag.makeTag(row)

      val tags: Map[String, Double] = appTags ++ deviceTags ++ regionTags  ++ channelTags ++ keyWordsTags ++ ageTags ++ sexTags  ++ businessAreaTags

      // 4.9 生成用户标识, 标识用户
      val ids: List[String] = getUserAllIds(row)

      // 返回
      (ids.head, (ids, tags))
    })

    // 10. 用户同一识别
    val graphRdd: RDD[(VertexId, (VertexId, (List[String], Map[String, Double])))] = UserGraphx.graph(tagRDD)

    // 11. 标签聚合
    val currentTagRdd: RDD[(String, (List[String], Map[String, Double]))] = TagAgg.aggTag(graphRdd)

    // 12. 历史标签聚合
    /**
      * |52:54:00:04:18:8e|52:54:00:04:18:8e...|BA_大溪沟 -> 1.0#DT_...|
      */
    // 读取历史数据
    val historyDF = spark.read
      .option("kudu.master", ConfUtils.MASTER)
      .option("kudu.table", TAG_SINK_TABLE)
      .kudu
    // 将数据转化为指定格式
    val historyTag: RDD[(String, (List[String], Map[String, Double]))] = historyDF.map(row=> {
      val userId = row.getAs[String]("userId")
      val allUserId: List[String] = row.getAs[String]("allUserId").split(",").toList
      val tags = row.getAs[String]("tags").split("#").map(line => {
        val arr = line.split(" -> ")
        val tagStr = arr.head
        val value = arr.last
        (tagStr, value.toDouble)
      }).toMap

      (userId, (allUserId, tags))
    }).rdd

    // 将历史数据和当天数据进行聚合
    val aggTagRdd = HistoryTag.merge(historyTag, currentTagRdd)
    // aggTagRdd.foreach(println(_))

    val kuduContext = new KuduContext(ConfUtils.MASTER, spark.sparkContext)
    // 将聚合的标签存放在kudu中
    val result: DataFrame = aggTagRdd.map{
      case (id, (allUserId, tags)) =>
        val userId: String = allUserId.mkString(",")
        val tagStr: String = tags.mkString("#")
        (id, userId, tagStr)
    }.toDF("userId", "allUserId", "tags")
    // 定义主键
    val keys = Seq[String]("userId")
    KuduUtils.write(TAG_TABLE, result.schema, result, keys, kuduContext)
    /*val result = tagsRdd.map{
      case (id, (allUserId, tags)) =>
        val userId = allUserId.mkString(",")
        val tagStr = tags.mkString("#")
        (id, userId, tagStr)
    }.toDF("userId", "allUserId", "tags")
    // 定义主键
    val keys = Seq[String]("userId")
    val kuduContext = new KuduContext(ConfUtils.MASTER, spark.sparkContext)
    KuduUtils.write(TAG_SINK_TABLE, result.schema, result, keys, kuduContext)*/
  }

  /**
    * 自定义udf函数 得到geoHashCode
    *
    * @param longitude 经度
    * @param latitude  维度
    * @return geoHashCode
    */
  def getGeoHashCode(longitude: Float, latitude: Float): String = {

    val geoHashCode = GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, 8)
    geoHashCode
  }


  /**
    * 获取用户标识非空的字段
    * @param row
    */
  def getUserAllIds(row:Row) ={
    var result = List[String]()

    val imei = row.getAs[String]("imei")
    val idfa = row.getAs[String]("idfa")
    val mac = row.getAs[String]("mac")
    val openudid = row.getAs[String]("openudid")
    val androidid = row.getAs[String]("androidid")

    if(StringUtils.isNotBlank(imei)) {
      result = result.+:((imei))
    }

    if(StringUtils.isNotBlank(idfa)) {
      result = result.+:((idfa))
    }

    if(StringUtils.isNotBlank(mac)) {
      result = result.+:((mac))
    }
    if(StringUtils.isNotBlank(openudid)) {
      result = result.+:((openudid))
    }

    if(StringUtils.isNotBlank(androidid)) {
      result = result.+:((androidid))
    }

    result
  }
}
