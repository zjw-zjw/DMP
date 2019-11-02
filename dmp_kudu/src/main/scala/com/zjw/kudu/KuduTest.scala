package com.zjw.kudu


import org.apache.kudu.client.{CreateTableOptions, KuduTable}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.junit.Test

class KuduTest {

  val spark = SparkSession.builder().appName("kudu").master("local[4]").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("warn")

  import spark.implicits._

  // Master地址和端口
  val MASTER = "node01:7051, node02:7051, node03:7051"

  // 构建KuduContext对象
  val kuduContext = new KuduContext(MASTER, sc)

  // 表名
  val TABLE_NAME = "student_1"


  /**
    * 创建表
    */
  @Test
  def create(): Unit = {
    // 定义表的schema
    val schema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
      .add("age", IntegerType)

    // 定义表的主键
    val primaryKey = Seq[String]("id")

    // 定义分区的schema
    val options = new CreateTableOptions
    // 指定分区字段
    val partition = Seq[String]("id")
    import scala.collection.JavaConversions._
    // 指定分区规则是hash 分区字段是id字段，分区数是3
    options.addHashPartitions(partition, 3)
    // 设置副本数
    options.setNumReplicas(1)

    // 创建表
    if(!kuduContext.tableExists(TABLE_NAME)) {
       kuduContext.createTable(TABLE_NAME, schema = schema, primaryKey, options)
    }

  }


  /**
    * 添加数据到表中
    */
  @Test
  def insert(): Unit = {

    var data = Seq[(Int, String, Int)]()
    for (i <- 1 to 10) {
      data = data.+:(i,"zhangsan-"+i,20+i)
    }

    val dataFrame = data.toDF("id", "name", "age")

    // 插入
    kuduContext.insertRows(data = dataFrame, tableName = TABLE_NAME)

  }


  /**
    * 查询表数据
    */
  @Test
  def query(): Unit = {

    val columns = Seq("id", "name", "age")

    val result: RDD[Row] = kuduContext.kuduRDD(sc, TABLE_NAME, columns)

    result.foreach(println(_))
  }


  /**
    * 更新数据
    */
  @Test
  def update(): Unit = {

    val data = Seq((1, "lisi"),(2, "wangwu")).toDF("id", "name")

    kuduContext.updateRows(data, TABLE_NAME)
  }


  /**
    * 删除数据
          注意:
          删除数据的时候只能用主键去删除，不能带上非主键的字段，如果带上则报错
          Invalid argument: DELETE should not have a value for column: name[string NULLABLE] (error 0)
    */
    @Test
  def delete(): Unit = {

    val column = Seq((5)).toDF("id")

    kuduContext.deleteRows(column, TABLE_NAME)
  }


  /**
    * 如果主键存在，则更新，如果主键不存在，则插入
    */
  @Test
  def upsert(): Unit = {

    val data = Seq((1, "zhaoliu", 44), (30, "sanshi", 30)).toDF("id", "name", "age")
    kuduContext.upsertRows(data, TABLE_NAME)
  }


  /**
    * 删除表
    */
  @Test
  def dropTable(): Unit = {
    if(kuduContext.tableExists("spark_kudu")){
      kuduContext.deleteTable("spark_kudu")
    }
  }


  /**
    * 使用SQL的声明式来读取
    */
  @Test
  def sqlRead(): Unit ={
    import  org.apache.kudu.spark.kudu._
    spark.read
      .option("kudu.master", MASTER)
      .option("kudu.table", TABLE_NAME)
      .kudu
      .show()
  }


  /**
    * sql写入目前只能支持SaveMode.Append模式
    */
  @Test
  def sqlWrite(): Unit = {
    //定义map集合，封装kudu的master地址和要读取的表名
    val options = Map (
      "kudu.master" -> MASTER,
      "kudu.table" -> TABLE_NAME
    )

    val data = List((21, "jj", 34), (22, "zz", 55))
    import  org.apache.kudu.spark.kudu._

    val df = sc.parallelize(data).toDF("id", "name", "age")
    // 将df的数据写入到kudu中
    df.write.mode("append").options(options).kudu

    // 展示结果
    spark.read
      .option("kudu.master", MASTER)
      .option("kudu.table", TABLE_NAME)
      .kudu
      .show()

  }



  @Test
  def SparkSql2Kudu(): Unit = {
    //定义map集合，封装kudu的master地址和表名
    val options = Map(
      "kudu.master" -> MASTER,
      "kudu.table" -> TABLE_NAME
    )
    // 定义数据
    val data =  List((24, "小张", 30), (25, "小王", 40))
    // 转化为dataframe
    val df = sc.parallelize(data).toDF()
    // 将df注册成一张表
    df.createTempView("temp1")

    // 获取kudu表的数据,然后注册成一张表
    import  org.apache.kudu.spark.kudu._
    spark.read.options(options = options).kudu.createTempView("temp2")

    // 使用sparkSQL的insert操作插入数据
    spark.sql("insert into temp2 select * from temp1")
    spark.sql("select * from temp2 where age > 30").show()
  }
}
