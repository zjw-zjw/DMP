package com.zjw.dmp.indicators

import com.zjw.dmp.utils.{ConfUtils, DateUtils, KuduUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 统计各省市的地域分布情况
  */
object ProvinceCityAnalysis {

  // 定义原数据读取表
  val SOURCE_TABLE = s"ODS_${DateUtils.getNow()}"
  // 定义数据存入表
  val SINK_TABLE = s"province_city_analysis_${DateUtils.getNow()}"

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

    import org.apache.kudu.spark.kudu._
    // 2. 读取ods层数据
    val source: DataFrame = spark.read
      .option("kudu.master", ConfUtils.MASTER)
      .option("kudu.table", SOURCE_TABLE)
      .kudu

    // 3. 列裁剪, 过滤
    source.selectExpr("province", "city")
        .filter("province is not null and province != '' and city is not null and city != ''")
      .createOrReplaceTempView("ods")

    // 4. 统计
    val result: DataFrame = spark.sql(
      """
        |select province, city, count(1) as count
        |from ods
        |group by province, city
      """.stripMargin)

    // 5. 保存结果到kudu中
    // 5.1 指定schema信息
    val schema = result.schema
    // 5.2 指定主键字段
    val keys = Seq("province", "city")
    val kuduContext = new KuduContext(ConfUtils.MASTER, spark.sparkContext)
    KuduUtils.write(SINK_TABLE, schema, result, keys, kuduContext)
  }
}
