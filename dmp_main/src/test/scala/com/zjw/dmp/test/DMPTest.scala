package com.zjw.dmp.test

import com.zjw.dmp.utils.{ConfUtils, DateUtils, IPAddressUtils, IPLocation}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.junit.Test

class DMPTest {

  val SOURCE_TABLE = s"business_area_${DateUtils.getNow()}"

  @Test
  def showTable(): Unit = {
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


    import  org.apache.kudu.spark.kudu._
    spark.read
      .option("kudu.master", ConfUtils.MASTER)
      .option("kudu.table", SOURCE_TABLE)
      .kudu
      .show()

  }


  /**
    * 测试工具类解析IP地址
    */
  @Test
  def getLongAndLat(): Unit = {
    val iPAddressUtils = new IPAddressUtils
    val ip = "171.14.161.255"
    val location: IPLocation = iPAddressUtils.getIPLocation(ip)
  }

  @Test
  def deleteTable(): Unit = {
    // 创建sparkSession
    val spark = SparkSession.builder().appName("etl").master("local[4]").getOrCreate()
    val kuduContext = new KuduContext(ConfUtils.MASTER, spark.sparkContext)
    kuduContext.deleteTable("ODS_()")
  }
}
