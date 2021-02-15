package com.yykj.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDJoin {
  /**
   * Join:标准操作[Transform &  Key-Value类型]
   * 在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素连接在一起的(K,(V,W))的RDD
   * def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 Join:在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素连接在一起的(K,(V,W))的RDD
    //transformJoin(sc)
    // TODO: 2.2 Join一对多
    transformJoin02(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * Join:在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素连接在一起的(K,(V,W))的RDD
   * @param sc
   */
  def transformJoin(sc: SparkContext): Unit = {
    val dataRDD_1: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("b", 2), ("c", 3)))
    val dataRDD_2: RDD[(String, Int)] = sc.makeRDD(Array(("a", 4), ("d", 10), ("c", 6)))
    val joinRDD = dataRDD_1.join(dataRDD_2)
    joinRDD.collect().foreach(println)
  }

  /**
   * Join:1对多 可能出现笛卡尔乘积 出现性能问题
   * @param sc
   */
  def transformJoin02(sc: SparkContext): Unit = {
    val dataRDD_1: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("b", 2), ("c", 3)))
    val dataRDD_2: RDD[(String, Int)] = sc.makeRDD(Array(("a", 4), ("a", 5), ("a", 6)))
    val joinRDD = dataRDD_1.join(dataRDD_2)
    joinRDD.collect().foreach(println)
  }
}
