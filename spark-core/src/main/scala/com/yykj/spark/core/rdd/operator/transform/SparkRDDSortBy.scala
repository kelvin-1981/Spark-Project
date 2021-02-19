package com.yykj.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDSortBy {

  /**
   * sortBY:标准操作[Transform & Value单值类型]
   * 该操作用于排序数据。在排序之前，可以将数据通过f函数进行处理，之后按照f函数处理的结果进行排序，默认为升序排列。
   * 排序后新产生的RDD的分区数与原RDD的分区数一致。中间存在shuffle的过程
   * def sortBy[K](f: (T) => K,ascending: Boolean = true,numPartitions: Int = this.partitions.length)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 sortBY: 该操作用于排序数据。在排序之前，可以将数据通过f函数进行处理，之后按照f函数处理的结果进行排序，默认为升序排列。
    transformSortBy(sc)

    // TODO: 2.2 sortBY: 元组形式排序
    //transformSortBy02(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * sortBY: 该操作用于排序数据。在排序之前，可以将数据通过f函数进行处理，之后按照f函数处理的结果进行排序，默认为升序排列。
   * @param sc
   */
  def transformSortBy(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(6, 3, 4, 5, 1, 2),2)
    val sortRDD: RDD[Int] = dataRDD.sortBy(num => num,false)
    sortRDD.saveAsTextFile("spark-core/datas/output/")
  }

  /**
   * sortBY: 该操作用于排序数据。在排序之前，可以将数据通过f函数进行处理，之后按照f函数处理的结果进行排序，默认为升序排列。
   * @param sc
   */
  def transformSortBy02(sc: SparkContext): Unit = {
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("A", 3), ("B", 2), ("C", 1)),2)
    val sortRDD: RDD[(String, Int)] = dataRDD.sortBy(_._2)
    sortRDD.collect().foreach(println)
  }
}
