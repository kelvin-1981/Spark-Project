package com.yykj.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDSubtract {

  /**
   * Subtract:标准操作[Transform & 双Value单值类型] 双Value类型指两个数据源的操作
   * 差集:以一个RDD元素为主，去除两个RDD中重复元素，将其他元素保留下来。
   * def subtract(other: RDD[T]): RDD[T]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 Subtract:差集:以一个RDD元素为主，去除两个RDD中重复元素，将其他元素保留下来。
    transformSubtract(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * Subtract差集:以一个RDD元素为主，去除两个RDD中重复元素，将其他元素保留下来。
   * @param sc
   */
  def transformSubtract(sc: SparkContext): Unit = {
    val dataRDD_1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    val dataRDD_2: RDD[Int] = sc.makeRDD(List(5, 6, 7, 8, 9, 10))

    // TODO: 1.需要以一个RDD为主
    val subRDD_1: RDD[Int] = dataRDD_1.subtract(dataRDD_2)
    subRDD_1.collect().foreach(println)
    println("-------------------------------------------")

    val subRDD_2: RDD[Int] = dataRDD_2.subtract(dataRDD_1)
    subRDD_2.collect().foreach(println)
  }
}
