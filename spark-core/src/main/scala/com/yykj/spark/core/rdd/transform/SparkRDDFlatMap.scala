package com.yykj.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDFlatMap {

  /**
   * flatMap:标准操作[Value集合类型]
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.执行操作
    // TODO: 2.1 flatMap标准操作[Value集合类型] 含简化写法
    transformFlatMap(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * mapPartitions标准操作[Value集合类型]
   * @param sc
   */
  def transformFlatMap(sc: SparkContext): Unit = {
    val dataRDD: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))

//    val flatRDD: RDD[Int] = dataRDD.flatMap(iter =>{
//      iter.map(_ * 10)
//    })

    val flatRDD: RDD[Int] = dataRDD.flatMap(_.map(_ * 10))
    flatRDD.foreach(println)
  }
}
