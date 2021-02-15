package com.yykj.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDGlom {

  /**
   * glom:标准操作[Transform & Value单值类型]
   * 将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
   * def glom(): RDD[Array[T]]
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.执行操作
    // TODO: 2.1 glom标准操作：将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
    transformGlom(sc)

    // TODO: 2.2 glom: 每个分区最大值求和计算
    //transformGlomPartitionMaxSum(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * glom:标准操作[Transform & Value单值类型]
   * 将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
   * @param sc
   */
  def transformGlom(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val glomRDD: RDD[Array[Int]] = dataRDD.glom()
    // TODO: 返回元素为数值格式 需要进行数组显示
    glomRDD.collect().foreach(data => {
      println(data.mkString(","))
    })
  }

  /**
   * glom: 每个分区最大值求和计算
   * @param sc
   */
  def transformGlomPartitionMaxSum(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val glomRDD: RDD[Array[Int]] = dataRDD.glom()
    val mapRDD: RDD[Int] = glomRDD.map(_.max)
    println(mapRDD.collect().sum)
  }
}
