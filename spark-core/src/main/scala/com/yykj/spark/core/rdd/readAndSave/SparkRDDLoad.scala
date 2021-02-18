package com.yykj.spark.core.rdd.readAndSave

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDLoad {

  /**
   * RDD 文件读取与保存
   * Spark的数据读取及数据保存可以从两个维度来作区分：文件格式以及文件系统。
   * 文件格式分为：text文件、csv文件、sequence文件以及Object文件；
   * 文件系统分为：本地文件系统、HDFS、HBASE以及数据库。
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.环境准备
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark")
    val sc = new SparkContext(conf)
    // TODO: 2.业务逻辑
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)))
    // TODO: 2.1 textFile\saveAsObjectFile\saveAsSequenceFile
    dataRDD.saveAsTextFile("spark-core/datas/output")
    dataRDD.saveAsObjectFile("spark-core/datas/output-2")
    dataRDD.saveAsSequenceFile("spark-core/datas/output-3")
    // TODO: 3.关闭环境
    sc.stop()
  }
}
