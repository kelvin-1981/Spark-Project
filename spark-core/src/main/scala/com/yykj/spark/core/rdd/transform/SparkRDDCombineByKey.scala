package com.yykj.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDCombineByKey {
  /**
   * combineByKey:标准操作[Transform &  Key-Value类型]
   * 最通用的对key-value型rdd进行聚集操作的聚集函数（aggregation function）。
   * 当计算时，发现数据结构不满足要求时，可以让第一个数据转换结构。分区内和分区间计算规则不相同。类似于aggregate()，combineByKey()允许用户返回值的类型与输入不一致。
   * def combineByKey[C](createCombiner: V => C,mergeValue: (C, V) => C,mergeCombiners: (C, C) => C): RDD[(K, C)]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 combineByKey:最通用的对key-value型rdd进行聚集操作的聚集函数（aggregation function）计算平均值
    transformCombineByKey(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * combineByKey:最通用的对key-value型rdd进行聚集操作的聚集函数（aggregation function）计算平均值
   * @param sc
   */
  def transformCombineByKey(sc: SparkContext): Unit = {
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4),("b", 5),("a", 6)), 2)
    // TODO: 1.combineByKey 最通用的对key-value型rdd进行聚集操作的聚集函数,将第一个元素进行数据结构转换
    // TODO: 2.此示例求平均值 需要计算两个数值（总值及次数）使用tuple分区内结果的记录 tuple _1,_2对应指标项自定义
    // TODO: 3.参数说明：
    // 参数1： 第一个参数的数据结构转换
    // 参数2： 分区内计算规则
    // 参数3： 分区间计算规则
    val combinRDD: RDD[(String, (Int, Int))] = dataRDD.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => (t._1 + v, t._2 + 1),
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    )
    val combineRDD: RDD[(String, Int)] = combinRDD.mapValues(tup => tup._1 / tup._2)
    combineRDD.collect().foreach(println)
  }
}
