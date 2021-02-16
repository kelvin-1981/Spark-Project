package com.yykj.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDRepartition {

  /**
   * Repartition:标准操作[Transform & Value单值类型 & shuffle算子]
   * 该操作内部其实执行的是coalesce操作，参数shuffle的默认值为true。无论是将分区数多的RDD转换为分区数少的RDD，
   * 还是将分区数少的RDD转换为分区数多的RDD，repartition操作都可以完成，因为无论如何都会经shuffle过程。
   * def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 repartition:增加或减少分区数量，必执行shuffle
    transformRepartition(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * coalesce:根据数据情况实现小数据集分区的合并(减少分区)，提升效率
   * @param sc
   */
  def transformRepartition(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6),2)
    val repartRDD = dataRDD.repartition(3)
    repartRDD.saveAsTextFile("spark-core/datas/output/")
  }
}
