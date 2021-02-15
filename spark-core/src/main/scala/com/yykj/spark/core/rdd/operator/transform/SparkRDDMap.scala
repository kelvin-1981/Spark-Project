package com.yykj.spark.core.rdd.operator.transform


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Iterator

/**
 *
 */
object SparkRDDMap {

  /**
   * Map[Transform & Value单值类型]
   * 将处理的数据逐条进行映射转换，这里的转换可以是类型的转换，也可以是值的转换。
   * def map[U: ClassTag](f: T => U): RDD[U]
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    // TODO: 2.执行操作
    // TODO: 2.1 Map算子 分区内数据处理：一个数据一个数据依次执行逻辑（详见MapPar）
    //transformMap(sc)
    // TODO: 2.2 Map算子[Value单值类型] 转换类型
    //transformMap02(sc)
    // TODO: 2.3 Map算子并行度
    transformMapPar(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * Map算子[Value单值类型]:遍历元素进行转换 转换数值 & 转换数值类型
   * @param sc
   */
  def transformMap(sc: SparkContext): Unit = {
    // TODO: 1.数据
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))

    // TODO: 2.执行
    //2.1 定义标准方法
    //def mapFunction(x: Int): Int = {
    //  x * 2
    //}
    //val mapRDD: RDD[Int] = dataRDD.map(mapFunction)

    //2.2 定义函数变量
    //var mapFunction: (Int) => Int = (x: Int) => {x * 2}
    //val mapRDD: RDD[Int] = dataRDD.map(mapFunction)

    //2.3 匿名函数(不能写入函数声明)
    //val mapRDD: RDD[Int] = dataRDD.map((x: Int) => {x * 2})

    //2.4 匿名函数(简化写法)
    val mapRDD: RDD[Int] = dataRDD.map(_ * 2)

    // TODO: 输出
    mapRDD.collect().foreach(println)
  }

  /**
   * Map算子[Value单值类型]:遍历元素进行转换 转换数值 & 转换数值类型
   * @param sc
   */
  def transformMap02(sc: SparkContext): Unit = {
    // TODO: 截取字符串示例
    val dataRDD: RDD[String] = sc.textFile("spark-core/datas/input/apache.log")

    val mapRDD: RDD[String] = dataRDD.map(line => {
      val list: Array[String] = line.split(" ")
      list(6)
    })

    mapRDD.collect().foreach(println)
  }

  /**
   * Map算子并行度
   * @param sc
   */
  def transformMapPar(sc: SparkContext): Unit = {
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
  }
}
