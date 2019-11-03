package com.zjw.dmp.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * 统一用户识别
  */
object UserGraphx {

  def graph(tagRDD: RDD[(String, (List[String], Map[String, Double]))]) ={
    // cache一下
    val newTagRdd = tagRDD.cache()
    // 1. 建立点
    val vertexRdd: RDD[(VertexId, (List[String], Map[String, Double]))] = newTagRdd.map{
      case (id, (allUserId, tags)) =>
        // 点的唯一Id
      val vertexId: VertexId = id.hashCode.toLong
        (vertexId, (allUserId, tags))
    }
    // 2. 建立边
    val edgeRdd: RDD[Edge[Int]] = newTagRdd.flatMap{
      case (id, (allUserId, tags)) =>
        val result = new ListBuffer[Edge[Int]]()

        // 边的源点的顶点Id
        val srcId: VertexId = id.hashCode.toLong
        allUserId.foreach(userId => {
          // 边的另一头的点的Id
          val dstId: VertexId = userId.hashCode.toLong
          result.+=(Edge(srcId, dstId, 0))
        })
        result
    }

    // 3. 生成图
    val graph = Graph(vertexRdd, edgeRdd)
    // 4. 得到连通图
    val connectedGraph: Graph[VertexId, Int] = graph.connectedComponents()
    // 5. 标签聚合
    val vertices: VertexRDD[VertexId] = connectedGraph.vertices
    // (id, aggId) join (id, (allUserId, tags)) => (id, (aggId, (allUserId, tags)))
    val joinRdd = vertices.join(vertexRdd)

    // 6. 数据返回
     joinRdd
  }
}
