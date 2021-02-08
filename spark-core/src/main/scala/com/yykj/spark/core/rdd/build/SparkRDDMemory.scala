package com.yykj.spark.core.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从内存中创建RDD
 */
object SparkRDDMemory {

  def main(args: Array[String]): Unit = {

    // TODO: 1.准备环境 *:CPU核数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    // TODO: 2.内存创建RDD
    var seq = Seq[Int](1,2,3,4,5)
    //创建方法1
    //var rdd : RDD[Int] = sc.parallelize(seq)
    //创建方法2 makeRDD底层调用parallelize方法
    var rdd: RDD[Int] = sc.makeRDD(seq)
    rdd.collect().foreach(println)

    // TODO: 3关闭环境
    sc.stop()
  }
}
