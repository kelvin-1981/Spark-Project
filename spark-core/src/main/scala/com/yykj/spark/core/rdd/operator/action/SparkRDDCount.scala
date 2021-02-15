package com.yykj.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDCount {

  /**
   * count:标准操作[Action] Action算子：出发作业（Job）执行的算子
   * 返回RDD中元素的个数
   * def count(): Long
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 count:返回RDD中元素的个数
    actionCount(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * collect:聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据
   * @param sc
   */
  def actionCount(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    val count: Long = dataRDD.count()
    println(count)
  }
}
