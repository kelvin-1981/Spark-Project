package com.yykj.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDTakeOrdered {

  /**
   * takeOrdered:标准操作[Action] Action算子：出发作业（Job）执行的算子
   * 返回该RDD排序后的前n个元素组成的数组
   * def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 takeOrdered:返回该RDD排序后的前n个元素组成的数组
    actionTakeOrdered(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * takeOrdered:返回该RDD排序后的前n个元素组成的数组
   * @param sc
   */
  def actionTakeOrdered(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    val values: Array[Int] = dataRDD.takeOrdered(2)(Ordering.Int.reverse)
    values.foreach(println)
  }
}
