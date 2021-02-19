package com.yykj.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDAggregate {

  /**
   * aggregate:标准操作[Action] Action算子：出发作业（Job）执行的算子
   * 分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
   * def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 aggregate:分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
    actionAggregate(sc)
    // TODO: 2.2 aggregate:aggregate与aggregateByKey初始值区别
    //actionAggregate02(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * aggregate:分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
   * @param sc
   */
  def actionAggregate(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val res: Int = dataRDD.aggregate(0)(_ + _, _ * _)
    println(res)
  }

  /**
   * aggregate:aggregate与aggregateByKey初始值区别
   * @param sc
   */
  def actionAggregate02(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // TODO: 1.aggregateByKey算子初始值只参与分区内计算
    // TODO: 2.aggregate算子初始值参与分区内计算并且参与分区间计算  10 + (10+1+2) + (10+3+4) = 40
    val res: Int = dataRDD.aggregate(10)(_ + _, _ + _)
    println(res)
  }
}
