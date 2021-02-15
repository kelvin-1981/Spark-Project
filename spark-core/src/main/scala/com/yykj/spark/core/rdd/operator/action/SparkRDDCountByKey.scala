package com.yykj.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDCountByKey {

  /**
   * countByKey:标准操作[Action] Action算子：出发作业（Job）执行的算子
   * 统计每种key的个数
   * def countByKey(): Map[K, Long]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 countByKey:统计每种key的个数
    //actionCountByKey(sc)
    // TODO: 2.2 countByValue:统计每个数值的个数
    actionCountByValue(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * countByKey:统计每种key的个数
   * @param sc
   */
  def actionCountByKey(sc: SparkContext): Unit = {
    val dataRDD: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))
    val res: collection.Map[Int, Long] = dataRDD.countByKey()
    res.foreach(println)
  }

  /**
   * countByValue:统计每个数值的个数
   * @param sc
   */
  def actionCountByValue(sc: SparkContext): Unit = {
    val dataRDD = sc.makeRDD(List("a", "b", "a", "b", "a", "a", "c"))
    val res: collection.Map[String, Long] = dataRDD.countByValue()
    res.foreach(println)
  }
}
