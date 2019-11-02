package com.zjw.dmp.utils

import com.zjw.dmp.etl.ETLProcess.SINK_TABLE
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.StructType

object KuduUtils {


  /**
    * 将数据添加到表中
   */
  def write(tableName:String, schema :StructType, dataFrame:DataFrame,
           keys:Seq[String], kuduContext:KuduContext): Unit = {

    // 判断表是否存在, 如果存在, 删除表, 因为可能存在写入一半报错，再次重复执行的时候会有数据重复或者主键冲突
    if(kuduContext.tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
    }

    val options = new CreateTableOptions
    // 指定分区规则是hash  分区数是3
    import scala.collection.JavaConversions._
    options.addHashPartitions(keys, 3)
    // 设置副本数
    options.setNumReplicas(1)
    // 创建表
    kuduContext.createTable(tableName, schema, keys, options)

    import org.apache.kudu.spark.kudu._
    // 插入数据
    dataFrame.write
      .mode(SaveMode.Append)
      .option("kudu.master", ConfUtils.MASTER)
      .option("kudu.table", tableName)
      .kudu
  }
}
