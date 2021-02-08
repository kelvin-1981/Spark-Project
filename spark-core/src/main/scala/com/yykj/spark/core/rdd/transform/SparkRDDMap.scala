package com.yykj.spark.core.rdd.transform


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Iterator

/**
 *
 */
object SparkRDDMap {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    // TODO: 2.执行操作
    // TODO: 2.1 Map算子[Value单值类型] 分区内数据处理：一个数据一个数据依次执行逻辑（详见MapPar）
    //transformMap(sc)
    transformMap02(sc)

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
}
