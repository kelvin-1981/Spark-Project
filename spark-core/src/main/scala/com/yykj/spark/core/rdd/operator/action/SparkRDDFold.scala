package com.yykj.spark.core.rdd.operator.action

import com.yykj.spark.core.rdd.operator.action.SparkRDDAggregate.actionAggregate02
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDFold {

  /**
   * fold:标准操作[Action] Action算子：出发作业（Job）执行的算子
   * 折叠操作，aggregate的简化版操作
   * def fold(zeroValue: T)(op: (T, T) => T): T
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 fold:折叠操作，aggregate的简化版操作
    //actionFold(sc)
    // TODO: 2.2 fold与foldByKey初始值区别
    actionFold02(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * aggregate:分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
   * @param sc
   */
  def actionFold(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val res: Int = dataRDD.fold(0)(_ + _)
    println(res)
  }

  /**
   * fold与foldByKey初始值区别
   * @param sc
   */
  def actionFold02(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // TODO: 1.foldByKey算子初始值只参与分区内计算
    // TODO: 2.fold算子初始值参与分区内计算并且参与分区间计算  10 + (10+1+2) + (10+3+4) = 40
    val res: Int = dataRDD.fold(10)(_ + _)
    println(res)
  }
}
