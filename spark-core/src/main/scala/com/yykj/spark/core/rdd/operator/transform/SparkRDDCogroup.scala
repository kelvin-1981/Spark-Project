package com.yykj.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDCogroup {
  /**
   * cogroup:标准操作[Transform &  Key-Value类型]
   * 在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
   * connect + group : 根据相同的key先分组后进行连接
   * def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 cogroup:在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
    transformCogroup(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * cogroup:在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
   * @param sc
   */
  def transformCogroup(sc: SparkContext): Unit = {
    val dataRDD_1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2)))
    val dataRDD_2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6), ("c", 7)))
    val congRDD = dataRDD_1.cogroup(dataRDD_2)
    congRDD.collect().foreach(println)
  }
}
