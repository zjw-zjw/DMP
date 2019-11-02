package com.zjw.dmp.utils

import com.typesafe.config.{Config, ConfigFactory}

object ConfUtils {
  // 加载配置文件
  val conf: Config = ConfigFactory.load()

  // #sparksql shuffle分区数设置
  val SPARK_SQL_SHUFFLE_PARTITIONS = conf.getString("spark.sql.shuffle.partitions")
  // sparksql 广播小表的大小限制,默认10M
  val SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD = conf.getString("spark.sql.autoBroadcastJoinThreshold")
  // spark shuffle是否压缩
  val SPARK_SHUFFLE_COMPRESS = conf.getString("spark.shuffle.compress")
  // spark shuffle拉取数据失败的时候最大重试次数
  val SPARK_SHUFFLE_IO_MAXRETRIES = conf.getString("spark.shuffle.io.maxRetries")
  // spark shuffle拉取数失败的时候，每次重试的时间间隔
  val SPARK_SHUFFLE_IO_RETRYWAIT = conf.getString("spark.shuffle.io.retryWait")
  // 广播变量压缩
  val SPARK_BROADCAST_COMPRESS = conf.getString("spark.broadcast.compress")
  // 指定spark序列化方式
  val SPARK_SERIALIZER = conf.getString("spark.serializer")
  // 指定spark执行与存储的内存比例
  val SPARK_MEMORY_FRACTION = conf.getString("spark.memory.fraction")
  // 指定spark存储的内存比例
  val SPARK_MEMORY_STORAGEFRACTION = conf.getString("spark.memory.storageFraction")
  // 指定spark core shuffle的分区数
  val SPARK_DEFAULT_PARALLELISM = conf.getString("spark.default.parallelism")
  // 指定spark是否启用推测机制
  val SPARK_SPECULATION = conf.getString("spark.speculation.flag")
  // 指定spark推测机制的启动时机，默认是中位数1.5倍
  val SPARK_SPECULATION_MULTIPLIER = conf.getString("spark.speculation.multiplier")

  // Kudumaster
  val MASTER = conf.getString("kudu.master")

  // 纯真数据库文件名
  val IP_FILE = conf.getString("IP_FILE")
  // 纯真数据库所在路径
  val INSTALL_DIR = conf.getString("INSTALL_DIR")
  // 经纬度解析文件
  val GEOLITECITY_DAT = conf.getString("GeoLiteCity.dat")

  // 获取商圈请求路径
  val URL = conf.getString("URL")

  // 字典文件地址
  val APPID_NAME = conf.getString("APPID_NAME")
  val DEVICEDIC = conf.getString("DEVICEDIC")
}
