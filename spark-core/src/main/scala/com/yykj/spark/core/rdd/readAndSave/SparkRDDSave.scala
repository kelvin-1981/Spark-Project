package com.yykj.spark.core.rdd.readAndSave

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDSave {

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
    // TODO: 2.业务逻辑 textFile\objectFile\sequenceFile
    val dataRDD: RDD[String] = sc.textFile("spark-core/datas/output")
    println("textFile: " + dataRDD.collect().mkString(","))

    val dataRDD_2: RDD[(String, Int)] = sc.objectFile[(String, Int)]("spark-core/datas/output-2")
    println("objectFile: " + dataRDD_2.collect().mkString(","))

    val dataRDD_3: RDD[(String, Int)] = sc.sequenceFile[String, Int]("spark-core/datas/output-3")
    println("sequenceFile: " + dataRDD_3.collect().mkString(","))
    
    // TODO: 3.关闭环境
    sc.stop()
  }
}
