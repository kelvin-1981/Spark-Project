package com.yykj.spark.core.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.Directory

/**
 * RDD 并行度 & 分区 设置方法
 */
object SparkRDDMemoryPar {

  def main(args: Array[String]): Unit = {

    // TODO: 1.准备环境 *:CPU核数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    // TODO: 2.RDD 并行度 & 分区 设置方法
    // 并行度 ：分区4个 物理CPU核数4核 并行度=4； 分区4个 物理CPU核数1核 并行度=1 此情况为并发
    // 第二个参数设置并行度 如不传递则根据当前运行环境最大CPU核数设置为默认并行数量
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //保存文件 运行前删除spark-core/datas/output/文件夹
    rdd.saveAsTextFile("spark-core/datas/output")

    println("Success...")

    // TODO: 3关闭环境
    sc.stop()
  }
}
