package com.yykj.spark.core.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读取文件创建RDD
 */
object SparkRDDFile {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    // TODO: 2.创建RDD
    //2.1.文件创建RDD
    //var rdd: RDD[String] = sc.textFile("spark-core/datas/input/word-data/1.txt")
    //rdd.collect().foreach(println)

    //2.2.目录创建RDD
    //var rdd: RDD[String] = sc.textFile("spark-core/datas/input/word-data")
    //rdd.collect().foreach(println)

    //2.3.HDFS创建RDD
    val rdd: RDD[String] = sc.textFile("hdfs://node21:9000/spark/core/hello.txt")
    rdd.collect().foreach(println)

    // TODO: 3.关闭环境
    sc.stop()
  }
}
