package com.yykj.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDLeftOuterJoin {

  /**
   * leftOuterJoin:标准操作[Transform &  Key-Value类型]
   * 类似于SQL语句的左外连接
   * def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 leftOuterJoin:类似于SQL语句的左外连接
    transformLeftOuterJoin(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * leftOuterJoin:类似于SQL语句的左外连接
   * @param sc
   */
  def transformLeftOuterJoin(sc: SparkContext): Unit = {
    val dataRDD_1: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("b", 2), ("c", 3)))
    val dataRDD_2: RDD[(String, Int)] = sc.makeRDD(Array(("a", 4), ("d", 10), ("c", 6)))
    val joinRDD: RDD[(String, (Int, Option[Int]))] = dataRDD_1.leftOuterJoin(dataRDD_2)
    joinRDD.collect().foreach(println)
  }
}
