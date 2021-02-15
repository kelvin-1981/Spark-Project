package com.yykj.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDFirst {

  /**
   * first:标准操作[Action] Action算子：出发作业（Job）执行的算子
   * 返回RDD中的第一个元素
   * def first(): T
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 first:返回RDD中的第一个元素
    actionFirst(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * first:返回RDD中的第一个元素
   * @param sc
   */
  def actionFirst(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    val first: Int = dataRDD.first()
    println(first)
  }
}
