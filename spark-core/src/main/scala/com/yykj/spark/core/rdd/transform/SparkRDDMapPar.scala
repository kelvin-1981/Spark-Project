package com.yykj.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD算子并行计算
 */
object SparkRDDMapPar {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    // TODO: 2.执行操作,并行计算
    /**2.1 一个分区内数据执行
     * 1)相同分区的数据执行一个分区内的数据依次执行全部逻辑
     * 2)只有前面数据执行全部逻辑后才执行下一个数据
     * 3)分区内数据执行是有序的
     * map1: >>>>>>1
     * map2: ******1
     * map1: >>>>>>2
     * map2: ******2
     * map1: >>>>>>3
     * map2: ******3
     */
    //val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),1)

    /**2.2 多个分区数据执行
     * 1)不同分区内数据执行是无序的
     * map1: >>>>>>3
     * map1: >>>>>>1
     * map2: ******3
     * map2: ******1
     * map1: >>>>>>4
     * map2: ******4
     * map1: >>>>>>2
     * map2: ******2
     */
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    val rdd_1 = dataRDD.map(num => {
      println("map1: >>>>>>" + num)
      num
    })

    val rdd_2 = rdd_1.map(num => {
      println("map2: ******" + num)
      num
    })

    rdd_2.collect


    // TODO: 3.关闭环境
    sc.stop()
  }
}
