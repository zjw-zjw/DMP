package com.zjw.dmp.test

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkGraph {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("graph").master("local[4]").getOrCreate()
    //1、构建点 (id,（属性）)
    /**
      * 1 张三18
      * 2 李四19
      * 3 王五20
      * 4 赵六21
      * 5 韩梅梅22
      * 6 李雷23
      * 7 小明24
      * 9 tom25
      * 10 jerry26
      * 11  ession 27
      */
    val vertices: RDD[(VertexId, (String, Int))] = spark.sparkContext.parallelize(Seq[(VertexId, (String, Int))](
      (1, ("张三", 18)),
      (2, ("李四", 19)),
      (3, ("王五", 20)),
      (4, ("赵六", 21)),
      (5, ("韩梅梅", 22)),
      (6, ("李雷", 23)),
      (7, ("小明", 24)),
      (9, ("tom", 25)),
      (10, ("jerry", 26)),
      (11, ("ession", 27))
    ))
    // 2. 构建边
    val edge: RDD[Edge[Int]] = spark.sparkContext.parallelize(Seq[Edge[Int]](
      Edge(1, 133, 0),
      Edge(2, 133, 0),
      Edge(3, 133, 0),
      Edge(4, 133, 0),
      Edge(5, 133, 0),
      Edge(4, 155, 0),
      Edge(5, 155, 0),
      Edge(6, 155, 0),
      Edge(7, 155, 0),
      Edge(9, 188, 0),
      Edge(10, 188, 0),
      Edge(11, 188, 0)
    ))
    // 构建图
    val graph: Graph[(String, Int), Int] = Graph(vertices, edge)

    val components: Graph[VertexId, Int] = graph.connectedComponents()


    val vers: VertexRDD[VertexId] = components.vertices

    vers.foreach(println(_))

    // (6,(1,(李雷,23)))
//    vers.join(vertices).map {
//      case (id, (aggId, (name, age))) => (aggId, (id, name, age))
//    }.groupByKey().foreach(println(_))

    /**
      * (1,CompactBuffer((4,赵六,21), (1,张三,18), (5,韩梅梅,22), (6,李雷,23), (2,李四,19), (3,王五,20), (7,小明,24)))
        (9,CompactBuffer((9,tom,25), (10,jerry,26), (11,ession,27)))
      */
  }
}
