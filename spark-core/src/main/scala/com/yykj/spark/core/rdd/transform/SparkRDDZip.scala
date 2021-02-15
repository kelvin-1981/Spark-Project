package com.yykj.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDZip {

  /**
   * zip:标准操作[Transform & 双Value类型] 双Value类型指两个数据源的操作
   * 将两个RDD中的元素，以键值对的形式进行合并。其中，键值对中的Key为第1个RDD中的元素，Value为第2个RDD中的相同位置的元素。
   * def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 zip:将两个RDD中的元素，以键值对的形式进行合并。其中，键值对中的Key为第1个RDD中的元素，Value为第2个RDD中的相同位置的元素。
    // 要求1：两个RDD的分区数量必须相等
    // 要求2：每个分区中元素数量必须相等
    transformZip(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * zip:将两个RDD中的元素，以键值对的形式进行合并。其中，键值对中的Key为第1个RDD中的元素，Value为第2个RDD中的相同位置的元素。
   * @param sc
   */
  def transformZip(sc: SparkContext): Unit = {
    // 要求1：两个RDD的分区数量必须相等
    // 要求2：每个分区中元素数量必须相等
    val dataRDD_1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    val dataRDD_2: RDD[Int] = sc.makeRDD(List(5, 6, 7, 8, 9, 10))
    val zipRDD = dataRDD_1.zip(dataRDD_2)
    zipRDD.collect().foreach(println)
  }
}
