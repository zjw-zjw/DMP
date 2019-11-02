package com.zjw.dmp.indicators

import com.zjw.dmp.utils.{ConfUtils, DateUtils, KuduUtils}
import org.apache.spark.sql.SparkSession

/**
  * 广告投放的网络运营商分布情况统计
  */
object AdOperatorAnalysis {
  // 定义原数据读取表
  val SOURCE_TABLE = s"ODS_${DateUtils.getNow()}"
  // 定义数据存入表
  val SINK_TABLE = s"ad_operate_analysis_${DateUtils.getNow()}"

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
    val source = spark.read
      .option("kudu.master", ConfUtils.MASTER)
      .option("kudu.table", SOURCE_TABLE)
      .kudu

    // 3. 列裁剪, 过滤, 去重
    source.selectExpr("ispname","adplatformproviderid", "requestmode", "processnode",
      "iseffective", "isbilling", "isbid", "iswin", "adorderid", "adcreativeid", "winprice", "adpayment")
      .filter("ispname is not null and ispname != ''")
      .createOrReplaceTempView("ods")

    // 4. 数据统计分析  统计原始请求、有效请求、广告请求、.....
    spark.sql(
      """
        |select
        | ispname,
        | sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as org_request_num,
        | sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as valid_request_num,
        | sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) as ad_request_num,
        | sum(case when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1 and isbid = 1 and adorderid != 0  then 1 else 0 end) as bid_num,
        | sum(case when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1 and iswin = 1 then 1 else 0 end) as bid_success_num,
        | sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as ad_show_num,
        | sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) as ad_click_num,
        | sum(case when requestmode = 2 and iseffective = 1 and isbilling = 1 then 1 else 0 end) as media_show_num,
        | sum(case when requestmode = 3 and iseffective = 1 and isbilling = 1 then 1 else 0 end) as media_click_num,
        | sum(case when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid > 200000 and adcreativeid >200000 then winprice/1000 else 0 end) as ad_consumption,
        | sum(case when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid > 200000 and adcreativeid >200000 then winprice/1000 else 0 end) as ad_cost
        | from ods
        | group by ispname
      """.stripMargin)
      .createOrReplaceTempView("tmp")

    // 4.2 在上表的基础上用 竞价成功数/参与竞价数 => 竞价成功率,  点击量/展示量 => 点击率
    val result = spark.sql(
      """
        |select t.*, bid_success_num / bid_num as bid_success_rate, media_click_num/media_show_num asclick_rate
        | from tmp as t
      """.stripMargin)

    // 5. 结果数据存入kudu
    // 5.1 指定schema信息
    val schema = result.schema
    // 5.2 指定主键字段
    val keys = Seq("ispname")
    val kuduContext = new KuduContext(ConfUtils.MASTER, spark.sparkContext)

    KuduUtils.write(SINK_TABLE, schema, result, keys, kuduContext)

  }
}
