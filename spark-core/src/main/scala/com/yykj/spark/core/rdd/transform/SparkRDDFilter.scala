package com.yykj.spark.core.rdd.transform

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDFilter {

  /**
   * filter:标准操作[Transform & Value单值类型]
   * 将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。
   * 当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出现数据倾斜。
   * def filter(f: T => Boolean): RDD[T]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 filter标准操作 将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃
    //transformsFilter(sc)

    // TODO: 2.2 filter算子：示例 从服务器日志数据apache.log中获取2015年5月17日的请求路径
    transformFilterSimple(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * filter算子：标准操作 将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃
   * @param sc
   */
  def transformsFilter(sc: SparkContext): Unit = {
    val dataRDD = sc.makeRDD(List(1, 2, 3, 4))
    val filterRDD: RDD[Int] = dataRDD.filter(_ % 2 == 0)
    filterRDD.collect().foreach(println)
  }

  /**
   * filter算子：示例 从服务器日志数据apache.log中获取2015年5月17日的请求路径
   * @param sc
   */
  def transformFilterSimple(sc: SparkContext): Unit = {
    // TODO: 1.数据RDD
    val dataRDD: RDD[String] = sc.textFile("spark-core/datas/input/apache.log",2)

    // TODO: 2.过滤数据
    val filterRDD: RDD[String] = dataRDD.filter(line => {
      val datas: Array[String] = line.split(" ")
      val dateStr = datas(3)
      dateStr.startsWith("17/05/2015")
    })

    // TODO: 3.获取路径
    val mapRDD: RDD[String] = filterRDD.map(line => {
      val datas: Array[String] = line.split(" ")
      datas(6)
    })

    // TODO: 4.输出
    mapRDD.collect().foreach(println)
  }
}
