package com.yykj.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDRightOuterJoin {

  /**
   * rightOuterJoin:标准操作[Transform &  Key-Value类型]
   * 类似于SQL语句的右外连接
   * def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 rightOuterJoin:类似于SQL语句的右外连接
    transformRightOuterJoin(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * rightOuterJoin:类似于SQL语句的右外连接
   * @param sc
   */
  def transformRightOuterJoin(sc: SparkContext): Unit = {
    val dataRDD_1: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("b", 2), ("c", 3)))
    val dataRDD_2: RDD[(String, Int)] = sc.makeRDD(Array(("a", 4), ("d", 10), ("c", 6)))
    val joinRDD: RDD[(String, (Option[Int], Int))] = dataRDD_1.rightOuterJoin(dataRDD_2)
    joinRDD.collect().foreach(println)
  }
}
