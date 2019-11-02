package com.zjw.dmp.etl

import com.maxmind.geoip.{Location, LookupService}
import com.zjw.dmp.utils.{ConfUtils, DateUtils, IPAddressUtils, KuduUtils}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * 解析ip，获取经纬度、省份、城市
  */
object ETLProcess {

  val SINK_TABLE = s"ODS_${DateUtils.getNow()}"

  def main(args: Array[String]): Unit = {

    // 创建sparkSession
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
    // 读取源数据  代码优化,persist
    val source: Dataset[Row] = spark.read.json("dmp_main/data/data.json").persist(StorageLevel.MEMORY_AND_DISK)
    // 将source表注册成临时表, 后续用于与ip解析数据进行Join,补充省份、城市、经纬度
    source.createOrReplaceTempView("source")

    // 列裁剪, 过滤, 去重
    val ips = source.selectExpr("ip")
      .filter("ip is not null and ip != ''")
      .distinct()
      .as[String]

    // 根据ip解析经纬度，省份、城市
    ips.map(ip => {
      // 解析经纬度
      val lookupService = new LookupService(ConfUtils.GEOLITECITY_DAT)
      val location: Location = lookupService.getLocation(ip)
      // 经度
      val longitude = location.longitude
      // 维度
      val latitude = location.latitude

      // 解析省份,城市
      val iPAddressUtils = new IPAddressUtils
      val regions = iPAddressUtils.getregion(ip)
      // 省份
      val province = regions.getRegion
      // 城市
      val city = regions.getCity
      (ip, longitude, latitude, province, city)
    })
      .toDF("ip", "longitude", "latitude", "province", "city")
      .createOrReplaceTempView("ip_info")


    // 将解析经纬度，省份、城市补充到源数据中
    // 将小表缓存起来
    spark.sql("cache table ip_info")
    val result = spark.sql(
      """
        |select
        |  s.ip,
        |  s.sessionid,
        |  s.advertisersid,
        |  s.adorderid,
        |  s.adcreativeid,
        |  s.adplatformproviderid,
        |  s.sdkversion,
        |  s.adplatformkey,
        |  s.putinmodeltype,
        |  s.requestmode,
        |  s.adprice,
        |  s.adppprice,
        |  s.requestdate,
        |  s.appid,
        |  s.appname,
        |  s.uuid,
        |  s.device,
        |  s.client,
        |  s.osversion,
        |  s.density,
        |  s.pw,
        |  s.ph,
        |  p.longitude,
        |  p.latitude,
        |  p.province,
        |  p.city,
        |  s.ispid,
        |  s.ispname,
        |  s.networkmannerid,
        |  s.networkmannername,
        |  s.iseffective,
        |  s.isbilling,
        |  s.adspacetype,
        |  s.adspacetypename,
        |  s.devicetype,
        |  s.processnode,
        |  s.apptype,
        |  s.district,
        |  s.paymode,
        |  s.isbid,
        |  s.bidprice,
        |  s.winprice,
        |  s.iswin,
        |  s.cur,
        |  s.rate,
        |  s.cnywinprice,
        |  s.imei,
        |  s.mac,
        |  s.idfa,
        |  s.openudid,
        |  s.androidid,
        |  s.rtbprovince,
        |  s.rtbcity,
        |  s.rtbdistrict,
        |  s.rtbstreet,
        |  s.storeurl,
        |  s.realip,
        |  s.isqualityapp,
        |  s.bidfloor,
        |  s.aw,
        |  s.ah,
        |  s.imeimd5,
        |  s.macmd5,
        |  s.idfamd5,
        |  s.openudidmd5,
        |  s.androididmd5,
        |  s.imeisha1,
        |  s.macsha1,
        |  s.idfasha1,
        |  s.openudidsha1,
        |  s.androididsha1,
        |  s.uuidunknow,
        |  s.userid,
        |  s.iptype,
        |  s.initbidprice,
        |  s.adpayment,
        |  s.agentrate,
        |  s.lomarkrate,
        |  s.adxrate,
        |  s.title,
        |  s.keywords,
        |  s.tagid,
        |  s.callbackdate,
        |  s.channelid,
        |  s.mediatype,
        |  s.email,
        |  s.tel,
        |  s.sex,
        |  s.age
        |from source s left join ip_info p
        |on s.ip = p.ip
      """.stripMargin)

    // 保存数据到ODS
    // 获得表的schema信息
    val schema = result.schema
    // 定义主键字段
    val keys = Seq("sessionid")
    val kuduContext = new KuduContext(ConfUtils.MASTER, spark.sparkContext)
    // 将数据存入kudu
    KuduUtils.write(SINK_TABLE, schema, result, keys, kuduContext)
  }
}
