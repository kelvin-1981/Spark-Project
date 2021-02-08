package com.yykj.spark.core.rdd.build

import com.yykj.spark.core.mock.SparkMockParallelRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD 分区数据分配
 */
object SparkRDDMemoryPar2 {

  def main(args: Array[String]): Unit = {

    // TODO: 1.准备环境 *:CPU核数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    // TODO: 2.RDD 分区数据分配
    // 如何分配数据 默认文件1内数据：1,2；文件2内数据：3,4
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 10)

    //保存文件 运行前删除spark-core/datas/output/文件夹
    rdd.saveAsTextFile("spark-core/datas/output")

    println("Success...")

    // TODO: 3关闭环境
    sc.stop()
  }
}
