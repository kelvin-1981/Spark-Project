package com.yykj.spark.core.rdd.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDDependency02 {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.环境准备
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    // TODO: 2.业务逻辑
    val rdd_line: RDD[String] = sc.textFile("spark-core/datas/input/word-data/1.txt")
    //打印血缘关系
    println(rdd_line.dependencies)
    println("---------------------")

    val wordRDD: RDD[String] = rdd_line.flatMap(_.split(" "))
    println(wordRDD.dependencies)
    println("---------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    println(mapRDD.dependencies)
    println("---------------------")

    val redRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    println(redRDD.dependencies)
    println("---------------------")

    redRDD.collect().foreach(println)

    // TODO: 3.关闭环境
    sc.stop()
  }
}
