package com.yykj.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDReduceByKey {

  /**
   * reduceByKey:标准操作[Transform & Key-Value类型] Key-Value类型：数据源必须是key-value类型
   * 可以将数据按照相同的Key对Value进行聚合
   * def reduceByKey(func: (V, V) => V): RDD[(K, V)]
   * def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 reduceByKey:可以将数据按照相同的Key对Value进行聚合
    transformReduceByKey(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * reduceByKey:可以将数据按照相同的Key对Value进行聚合
   * @param sc
   */
  def transformReduceByKey(sc: SparkContext): Unit = {
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))
    // TODO: Spark基于Scala开发，Scala聚合一般采用两两聚合
    // TODO: 如果key("b")的value只有一个，则不会进行两两计算
    //val redRDD: RDD[(String, Int)] = dataRDD.reduceByKey((x: Int, y: Int) => (x + y))
    val redRDD: RDD[(String, Int)] = dataRDD.reduceByKey(_ + _)
    redRDD.collect().foreach(println)
  }
}
