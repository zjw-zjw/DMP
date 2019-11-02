package com.zjw.dmp.test

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Test

import scala.util.Random

/**
  * 哪些情况会出现数据倾斜:
  *   1、shuffle的时候，如果这个产生shuffle的字段为空，会出现数据倾斜
  *   2、key有很多，分区数设置的过少，导致很多key聚集在一个分区出现数据倾斜
  *   3、当某一个表中某一个key数据特别多，然后使用group by 就会出现数据倾斜
  *   4、大表 join 小表 ,这两个表中某一个表有某一个key或者某几个key数据比较多，会出现数据倾斜
  *   5、大表 join 大表，其中某一个表分布比较均匀，另一个表存在某一个或者某几个key数据特别多，也会出现数据倾斜
  *   6、大表 join 大表，其中某一个表分布比较均匀，另一个表存在很多key数据特别多,也会出现数据倾斜
  */
class dataProcess extends Serializable {

  val spark = SparkSession
    .builder()
    .config("spark.sql.autoBroadcastJoinThreshold","10485760")
    .master("local[4]")
    .appName("dataprocess").getOrCreate()

  import spark.implicits._
  /**
    * 1、shuffle的时候，如果这个产生shuffle的字段为空，会出现数据倾斜
    *   解决方案：
    *      将空字段进过滤
    */
  @Test
  def solution1(): Unit ={

    spark.sparkContext.parallelize(Seq[(Int,String,Int,String)](
      (1,"aa",20,""),
      (2,"bb",20,""),
      (3,"vv",20,""),
      (4,"dd",20,""),
      (5,"ee",20,""),
      (6,"ss",20,""),
      (7,"uu",20,""),
      (8,"qq",20,""),
      (9,"ww",20,""),
      (10,"rr",20,""),
      (11,"tt",20,""),
      (12,"xx",20,"class_02"),
      (13,"kk",20,"class_03"),
      (14,"oo",20,"class_01"),
      (15,"pp",20,"class_01")
    )).toDF("id","name","age","clazzId")
      .filter("clazzId is not null and clazzId!=''")    // 空值过滤
      .createOrReplaceTempView("student")

    spark.sparkContext.parallelize(Seq[(String,String)](
      ("class_01","java就业班"),
      ("class_02","python就业班"),
      ("class_03","大数据就业班")
    )).toDF("id","name")
        .createOrReplaceTempView("class_info")

    spark.sql(
      """
        |select s.id,s.name,s.age,c.name
        | from student s left join class_info c
        | on s.clazzId = c.id
      """.stripMargin)
  }

  /**
    * 当某一个表中某一个key数据特别多，然后使用group by 就会出现数据倾斜
    * 解决方案： 局部聚合+全局聚合
    */
  @Test
  def solution2(): Unit ={

    val source = spark.sparkContext.parallelize(Seq[(Int,String,Int,String)](
      (1,"aa",20,"class_01"),
      (2,"bb",20,"class_01"),
      (3,"vv",20,"class_01"),
      (4,"dd",20,"class_01"),
      (5,"ee",20,"class_01"),
      (6,"ss",20,"class_01"),
      (7,"uu",20,"class_01"),
      (8,"qq",20,"class_01"),
      (9,"ww",20,"class_01"),
      (10,"rr",20,"class_02"),
      (11,"tt",20,"class_02"),
      (12,"xx",20,"class_02"),
      (13,"kk",20,"class_03"),
      (14,"oo",20,"class_01"),
      (15,"pp",20,"class_01")
    )).toDF("id","name","age","clazzId")
    //.createOrReplaceTempView("student")

    //局部聚合 -> 将分组字段的值加上一个随机数[加盐] -> 然后进行局部聚合
    // 定义加盐函数
    val addPrefix = (id:String) => s"${id}#${Random.nextInt(10)}"
    /*    spark.sql(
          """
            |select clazzId,count(1)
            | from student
            | group by clazzId
          """.stripMargin)*/

    val tmp = spark.sql(
      """
        |select clazzId,count(1) cn
        | from student s
        | group by clazzId
      """.stripMargin)
    spark.udf.register("addPrefix",addPrefix)

    source.selectExpr("id","name","age","addPrefix(clazzId) clazzId").createOrReplaceTempView("student")


    //全局聚合
    val unprefix = (id:String)=> id.split("#").head
    spark.udf.register("unprefix",unprefix)

    tmp.selectExpr("unprefix(clazzId) clazzId","cn").createOrReplaceTempView("tmp")

    spark.sql("select clazzId,sum(cn) from tmp group by clazzId").show

  }

  /**
    * 大表 join 小表 ,这两个表中某一个表有某一个key或者某几个key数据比较多，会出现数据倾斜
    * 解决方案: 将小表广播出去，由原来的reduce join变成map join，避免shuffle操作，从而解决数据倾斜
    */
  @Test
  def solution3(): Unit ={

    val source = spark.sparkContext.parallelize(Seq[(Int,String,Int,String)](
      (1,"aa",20,"class_01"),
      (2,"bb",20,"class_01"),
      (3,"vv",20,"class_01"),
      (4,"dd",20,"class_01"),
      (5,"ee",20,"class_01"),
      (6,"ss",20,"class_01"),
      (7,"uu",20,"class_01"),
      (8,"qq",20,"class_01"),
      (9,"ww",20,"class_01"),
      (10,"rr",20,"class_02"),
      (11,"tt",20,"class_02"),
      (12,"xx",20,"class_02"),
      (13,"kk",20,"class_03"),
      (14,"oo",20,"class_01"),
      (15,"pp",20,"class_01")
    )).toDF("id","name","age","clazzId")
    .createOrReplaceTempView("student")


    spark.sparkContext.parallelize(Seq[(String,String)](
      ("class_01","java就业班"),
      ("class_02","python就业班"),
      ("class_03","大数据就业班")
    )).toDF("id","name")
      .createOrReplaceTempView("class_info")

    // 将小表cache, 进行广播
    spark.sql("cache table class_info")

    spark.sql(
      """
        |select s.id,s.name,s.age,c.name
        | from student s left join class_info c
        | on s.clazzId = c.id
      """.stripMargin)
      .rdd
      .mapPartitionsWithIndex((index,it)=>{
        println(s"index:${index}  data:${it.toBuffer}")
        it
      }).collect()
  }

  /**
    * 大表 join 大表，其中某一个表分布比较均匀，另一个表存在某一个或者某几个key数据特别多，也会出现数据倾斜
    * 解决方案步骤:
    *    1、将产生数据倾斜的key过滤出来单独处理[加盐]
    *    2、没有数据倾斜key的数据照常处理
    */
  @Test
  def solution4(): Unit ={

    val source = spark.sparkContext.parallelize(Seq[(Int,String,Int,String)](
      (1,"aa",20,"class_01"),
      (2,"bb",20,"class_01"),
      (3,"vv",20,"class_01"),
      (4,"dd",20,"class_01"),
      (5,"ee",20,"class_01"),
      (6,"ss",20,"class_01"),
      (7,"uu",20,"class_01"),
      (8,"qq",20,"class_01"),
      (9,"ww",20,"class_01"),
      (10,"rr",20,"class_02"),
      (11,"tt",20,"class_02"),
      (12,"xx",20,"class_02"),
      (13,"kk",20,"class_03"),
      (14,"oo",20,"class_01"),
      (15,"pp",20,"class_01")
    )).toDF("id","name","age","clazzId")
      //.createOrReplaceTempView("student")


    val clazz = spark.sparkContext.parallelize(Seq[(String,String)](
      ("class_01","java就业班"),
      ("class_02","python就业班"),
      ("class_03","大数据就业班")
    )).toDF("id","name")


    //通过采样得知产生数据倾斜的key
    //source.sample(false,0.6).show

    //1、从产生数据倾斜表中过滤出产生数据倾斜key
    val dataSolutionDF = source.filter("clazzId='class_01'")
    //2、从产生数据倾斜表中过滤出没有产生数据倾斜的key
    val studentDF = source.filter("clazzId!='class_01'")
    //3、从没有产生数据倾斜表中过滤出产生数据倾斜key
    val clazzDataSolutionDF = clazz.filter("id='class_01'")
    //4、从没有产生数据倾斜表中过滤出没有产生数据倾斜的key
    val clazzDF = clazz.filter("id!='class_01'")
    //5、对于没有产生数据倾斜key的数据照常join
    studentDF.createOrReplaceTempView("student")
    clazzDF.createOrReplaceTempView("clazz")

    spark.sql(
      """
        |select s.id,s.name,s.age,c.name
        | from student s left join clazz c
        | on s.clazzId = c.id
      """.stripMargin).createOrReplaceTempView("tmp1")

    //6、对于产生数据倾斜表中数据倾斜key进行加盐处理
    val addPrefix = (id:String) => s"${id}#${Random.nextInt(10)}"
    spark.udf.register("addPrefix",addPrefix)
    dataSolutionDF.selectExpr("id","age","name","addPrefix(clazzId) clazzId")
      .createOrReplaceTempView("student_tmp")
    //7、没有产生数据倾斜表中过滤出产生数据倾斜key的数据进行扩容

    capacity(clazzDataSolutionDF).createOrReplaceTempView("class_tmp")
    //8、join

    spark.sql(
      """
        |select s.id,s.name,s.age,c.name
        | from student_tmp s left join class_tmp c
        | on s.clazzId = c.id
      """.stripMargin).createOrReplaceTempView("tmp2")

    // 将两个分别join好的表进行合并  使用 union
    spark.sql(
      """
        |select * from tmp1
        |union
        |select * from tmp2
      """.stripMargin).show

  }

  /**
    * 大表 join 大表，其中某一个表分布比较均匀，另一个表存在很多key数据特别多,也会出现数据倾斜
    * 解决方案:
    *    直接对产生数据倾斜的表进行加盐[0-9]，对另一个表进行扩容[最多扩容10倍]
    */
  @Test
  def solution6(): Unit ={
    val source = spark.sparkContext.parallelize(Seq[(Int,String,Int,String)](
      (1,"aa",20,"class_01"),
      (2,"bb",20,"class_01"),
      (3,"vv",20,"class_01"),
      (4,"dd",20,"class_01"),
      (5,"ee",20,"class_01"),
      (6,"ss",20,"class_01"),
      (7,"uu",20,"class_01"),
      (8,"qq",20,"class_01"),
      (9,"ww",20,"class_01"),
      (10,"rr",20,"class_02"),
      (11,"tt",20,"class_02"),
      (12,"xx",20,"class_02"),
      (13,"kk",20,"class_03"),
      (14,"oo",20,"class_01"),
      (15,"pp",20,"class_01")
    )).toDF("id","name","age","clazzId")
    //.createOrReplaceTempView("student")


    val clazz = spark.sparkContext.parallelize(Seq[(String,String)](
      ("class_01","java就业班"),
      ("class_02","python就业班"),
      ("class_03","大数据就业班")
    )).toDF("id","name")

    //1、对产生数据倾斜的表添加随机数
    val addPrefix = (id:String) => s"${id}#${Random.nextInt(10)}"
    spark.udf.register("addPrefix",addPrefix)
    source.selectExpr("id","name","age","addPrefix(clazzId) clazzId").createOrReplaceTempView("student")

    //2、对没有产生数据倾斜的表进行扩容
    capacity(clazz).createOrReplaceTempView("clazz")

    spark.sql(
      """
        |select s.id,s.name,s.age,c.name
        | from student s left join clazz c
        | on s.clazzId = c.id
      """.stripMargin).show

  }
  /**
    * 对dataFrame进行扩容
    */
  def capacity(clazzDataSolutionDF:DataFrame) ={

    //1、创建空的DataFrame,其schema必须与clazzDataSolutionDF必须一样
    val emptyRdd = spark.sparkContext.emptyRDD[Row]
    var emptyDF = spark.createDataFrame(emptyRdd,clazzDataSolutionDF.schema)
    //2、0-9,给classId添加后缀
    spark.udf.register("prefix",prefix _)
    for(i <- 0 until(10)){
    //3、将当前dataFrame追加到创建的dataFrame中
      emptyDF = emptyDF.union(clazzDataSolutionDF.selectExpr(s"prefix(${i},id) id","name"))
    }

    emptyDF
  }

  /**
    * 给id添加固定的后缀
    * @param i
    * @param id
    */
  def prefix(i:Int,id:String)={
    s"${id}#${i}"
  }

}
