package com.yykj.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDFoldByKey {

  /**
   * foldByKey:标准操作[Transform & Key-Value类型]
   * 当分区内计算规则和分区间计算规则相同时，aggregateByKey就可以简化为foldByKey
   * def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 foldByKey:当分区内计算规则和分区间计算规则相同时，aggregateByKey就可以简化为foldByKey
    transformfoldByKey(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * AggregateByKey:将数据根据不同的规则进行分区内计算和分区间计算
   * @param sc
   */
  def  transformfoldByKey(sc: SparkContext): Unit = {
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)
    val foldRDD: RDD[(String, Int)] = dataRDD.foldByKey(0)(_ + _)
    foldRDD.collect().foreach(println)
  }
}
