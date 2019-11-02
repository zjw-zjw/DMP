package com.zjw.dmp.agg

import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

/**
  * 标签聚合
  */
object TagAgg {

  def aggTag(graphRdd: RDD[(VertexId, (VertexId, (List[String], Map[String, Double])))]) ={

    /**
        (-1137215488,(-1137215488,(List(UAPNCKVQQSTHXDUVYIXJPGLWEXSWOQEXYMVBIIQQ, UZWQSMMBFWOMCXZVYKTQXBKQLPFYKGUU),Map(CT_广西柳州市柳北区 -> 1.0, DT_D00010001 -> 1.0, KW_美文 -> 1.0, PV_广西柳州市柳北区 -> 1.0, APP_嘟嘟美甲 -> 1.0, BA_朝阳 -> 1.0, ISP_D00030003 -> 1.0, AGE_34 -> 1.0, CN_123477 -> 1.0, BA_民生 -> 1.0, DV_HUAWEI P9/P9 Plus -> 1.0, NW_手机 -> 1.0, SEX_男 -> 1.0, NW_D00020003 -> 1.0, BA_华强 -> 1.0))))
        (-1137215488,(-1137215488,(List(UAPNCKVQQSTHXDUVYIXJPGLWEXSWOQEXYMVBIIQQ, UZWQSMMBFWOMCXZVYKTQXBKQLPFYKGUU),Map(CT_广西柳州市柳北区 -> 1.0, DT_D00010001 -> 1.0, KW_美文 -> 1.0, PV_广西柳州市柳北区 -> 1.0, APP_嘟嘟美甲 -> 1.0, BA_朝阳 -> 1.0, ISP_D00030003 -> 1.0, AGE_34 -> 1.0, CN_123477 -> 1.0, BA_民生 -> 1.0, DV_HUAWEI P9/P9 Plus -> 1.0, NW_手机 -> 1.0, SEX_男 -> 1.0, NW_D00020003 -> 1.0, BA_华强 -> 1.0))))
        (12786809,(-1137215488,(List(PFTYZSYDARSCRMIMGBOJNLLMLWFIMBYJTYLHJIRC, UZWQSMMBFWOMCXZVYKTQXBKQLPFYKGUU),Map(DV_BLACK BARRY -> 1.0, KW_美文 -> 1.0, NW_D00020005 -> 1.0, KW_婚姻 -> 1.0, ISP_D00030002 -> 1.0, AGE_34 -> 1.0, CT_镇江市 -> 1.0, BA_北海 -> 1.0, KW_情感 -> 1.0, CN_123536 -> 1.0, PV_江苏省 -> 1.0, DT_D00010004 -> 1.0, BA_沙滩 -> 1.0, NW_手机 -> 1.0, SEX_男 -> 1.0, APP_陌陌 -> 1.0))))
      */
    // 1. 将aggId 作为二元元祖的Key
    val mapRdd = graphRdd.map {
      case (id, (aggId, (allUserId, tags))) => (aggId, (allUserId, tags))
    }

    // 2. 根据aggId分组聚合
    val groupedRdd: RDD[(VertexId, Iterable[(List[String], Map[String, Double])])] = mapRdd.groupByKey()

    // (-213743295,CompactBuffer(
    // (List(52:54:00:cb:ab:39, LRFXOONJCMNMQGPPWRMNCEDKZPXQNPFZ),Map(DV_BLACK BARRY -> 1.0, PV_河南省 -> 1.0, KW_重剑 -> 1.0, CT_河南省 -> 1.0, KW_花木兰 -> 1.0, KW_关羽 -> 1.0, ISP_D00030002 -> 1.0, KW_吕布 -> 1.0, NW_D00020002 -> 1.0, DT_D00010004 -> 1.0, CN_123553 -> 1.0, APP_团车 -> 1.0, KW_王者荣耀 -> 1.0, AGE_53 -> 1.0, NW_手机 -> 1.0, SEX_男 -> 1.0)),
    // (List(52:54:00:cb:ab:39, VDQJZSNGMRSRSNMALFULNXUTDGSVNOKB),Map(DT_D00010001 -> 1.0, KW_美文 -> 1.0, PV_广东省 -> 1.0, APP_搜狐 -> 1.0, CN_123523 -> 1.0, CT_茂名市 -> 1.0, ISP_D00030003 -> 1.0, BA_北海 -> 1.0, NW_D00020002 -> 1.0, BA_沙滩 -> 1.0, DV_HUAWEI P9/P9 Plus -> 1.0, AGE_53 -> 1.0, NW_手机 -> 1.0, SEX_男 -> 1.0)),
    // (List(PQSEGKZVRWRQZSPZLRRAYPYCOJUULTBNUUVKNZLM, 52:54:00:cb:ab:39, PWZTVLKQZPKWGVOKVDKNBNPHZOXCTXZA),Map(DV_BLACK BARRY -> 1.0, KW_美文 -> 1.0, CN_123541 -> 1.0, ISP_D00030003 -> 1.0, BA_颛桥 -> 1.0, CT_上海市 -> 1.0, PV_上海市 -> 1.0, DT_D00010004 -> 1.0, APP_脉脉 -> 1.0, KW_童话 -> 1.0, AGE_53 -> 1.0, NW_手机 -> 1.0, SEX_男 -> 1.0, NW_D00020003 -> 1.0))))

    // 3. 标签聚合
    val result: RDD[(String, (List[String], Map[String, Double]))] = groupedRdd.map {
      case (aggId, it) =>
        /**  it:
                List(52:54:00:cb:ab:39, LRFXOONJCMNMQGPPWRMNCEDKZPXQNPFZ)
                List(52:54:00:cb:ab:39, VDQJZSNGMRSRSNMALFULNXUTDGSVNOKB)
                List(PQSEGKZVRWRQZSPZLRRAYPYCOJUULTBNUUVKNZLM, 52:54:00:cb:ab:39, PWZTVLKQZPKWGVOKVDKNBNPHZOXCTXZA)
          */
        // 获取用户标识,去重
        val userId: List[String] = it.flatMap(_._1).toList.distinct
        // 对标签聚合
        val tags: Map[String, Double] = it.flatMap(_._2)
          // (标签名, [(标签名, 1), (标签名, 1), ...])
        .groupBy(_._1)
        .map(item => (item._1, item._2.map(_._2).sum))

      // 返回: (用户标识id, (用户标识Id数组, tags标签)
      (userId.head, (userId, tags))
    }

    result
  }
}
