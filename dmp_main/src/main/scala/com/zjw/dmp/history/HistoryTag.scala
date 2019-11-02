package com.zjw.dmp.history

import com.zjw.dmp.agg.TagAgg
import com.zjw.dmp.graphx.UserGraphx
import org.apache.spark.graphx.{GraphXUtils, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

/**
  * 历史标签与当天聚合
  */
object HistoryTag {

  def merge(historyTag: RDD[(String, (List[String], Map[String, Double]))],
            currentTagRdd: RDD[(String, (List[String], Map[String, Double]))]) ={

    // 1. 标签衰减
    /**
      * 牛顿冷却定律：当前温度=初始温度*exp(-冷却系数*时间)
      *   公式: 当前权重 = 历史权重 * 衰减系数[每种标签都有不同的衰减系数][0.9]
      */
    val newHistoryTag = historyTag.map {
      case (id, (allUserId, tags)) =>
      val newTags = tags.map(item => (item._1, item._2 * 0.9))
        (id, (allUserId, newTags))
    }
    // 2. 合并
    val unionRdd = newHistoryTag.union(currentTagRdd)

    // 3. 统一用户识别
    val garphRdd: RDD[(VertexId, (VertexId, (List[String], Map[String, Double])))] = UserGraphx.graph(unionRdd)
    // 3. 标签聚合
    val aggRdd: RDD[(String, (List[String], Map[String, Double]))] = TagAgg.aggTag(garphRdd)

    aggRdd
  }
}
