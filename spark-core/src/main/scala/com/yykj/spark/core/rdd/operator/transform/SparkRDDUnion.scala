package com.yykj.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDUnion {

  /**
   * union:标准操作[Transform & 双Value单值类型] 双Value类型指两个数据源的操作
   * 并集:对源RDD和参数RDD求并集后返回一个新的RDD
   * def union(other: RDD[T]): RDD[T]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 union:并集:对源RDD和参数RDD求并集后返回一个新的RDD
    transformUnion(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * union:并集:对源RDD和参数RDD求并集后返回一个新的RDD
   * @param sc
   */
  def transformUnion(sc: SparkContext): Unit = {
    val dataRDD_1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    val dataRDD_2: RDD[Int] = sc.makeRDD(List(5, 6, 7, 8, 9, 10))
    val unionRDD = dataRDD_1.union(dataRDD_2)
    unionRDD.collect().foreach(println)
  }
}
