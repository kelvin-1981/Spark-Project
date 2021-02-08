package com.yykj.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDMapPartitionsWithIndex {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.执行操作
    // TODO: 2.1 mapPartitionsWithIndex[Value集合类型]：根据需要读取的分区索引，读取单个分区的全部数据后进行逻辑处理
    //transformMapPartitionsWithIndex(sc)

    // TODO: 2.2 mapPartitionsWithIndex:显示数据所属分区
    transformMapPartitionsWithIndex02(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * MapPartitionsWithIndex：选择分区数据进行处理
   * @param sc
   */
  def transformMapPartitionsWithIndex(sc: SparkContext): Unit = {
    val dataRDD = sc.makeRDD(List(1, 2, 3, 4),2)
    val mapRDD: RDD[Int] = dataRDD.mapPartitionsWithIndex((index, iter) => {
      if(index == 1){ //只处理索引1分区的数据
        iter
      }
      else {
        Nil.iterator
      }
    })
    mapRDD.collect().foreach(println)
  }

  /**
   * MapPartitionsWithIndex：查看数据所属分区
   * @param sc
   */
  def transformMapPartitionsWithIndex02(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
//    val mapRDD: RDD[Int] = dataRDD.mapPartitionsWithIndex((index, iter) => {
//      iter.map(value => {
//        println("数据: " + value + " 分区索引: " + index)
//        value
//      })
//    })
//    mapRDD.collect

//    val mapRDD: RDD[(Int, Int)] = dataRDD.mapPartitionsWithIndex((index, iter) => {
//      iter.map((_, index))
//    })
//
//    mapRDD.foreach(tup => {
//      println("数据: " + tup._1 + " 分区索引: " + tup._2)
//    })

    val mapRDD: RDD[String] = dataRDD.mapPartitionsWithIndex((index, iter) => {
      iter.map("data: " + _ + " index: " + index)
    })
    mapRDD.collect().foreach(println)
  }
}
