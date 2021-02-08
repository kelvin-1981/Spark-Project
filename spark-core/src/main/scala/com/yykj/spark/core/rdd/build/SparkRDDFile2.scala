package com.yykj.spark.core.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 显示文件及文件内容创建RDD
 */
object SparkRDDFile2 {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    // TODO: 2.创建RDD
    //2.1.文件创建RDD,wholeTextFiles:以文件为单位读取数据 结果为元组格式 1：文件名称 2：文件内的数据
    val rdd : RDD[(String, String)] = sc.wholeTextFiles("spark-core/datas/input/word-data/")
    rdd.collect().foreach(println)

    // TODO: 3.关闭环境
    sc.stop()
  }
}
