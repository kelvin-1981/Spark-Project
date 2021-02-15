package com.yykj.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDDistinct {

  /**
   * distinct:标准操作[Transform & Value单值类型]
   * 将数据集中重复的数据去重
   * def distinct()(implicit ord: Ordering[T] = null): RDD[T]
   * def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 distinct:将数据集中重复的数据去重 实现思路如下
    // 底层代码：map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    // 实现过程：
    // (1, null),(2, null),(3, null),(4, null),(1, null),(2, null),(3, null),(4, null)
    // (1, null)(1, null)(1, null)
    // (null, null) => null
    // (1, null) => 1
    transformDistinct(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * distinct:将数据集中重复的数据去重
   * @param sc
   */
  def transformDistinct(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))

    // TODO: 1.将数据集中重复的数据去重
    // 底层代码：map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    // 实现过程：
    // (1, null),(2, null),(3, null),(4, null),(1, null),(2, null),(3, null),(4, null)
    // (1, null)(1, null)(1, null)
    // (null, null) => null
    // (1, null) => 1
    val distRDD: RDD[Int] = dataRDD.distinct()
    distRDD.collect().foreach(println)
    println("-----------------------------------")
  }
}
