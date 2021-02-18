package com.yykj.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object SparkAccumulator {

  /**
   * 1.累加器：分布式共享只写变量：用来把Executor端变量信息聚合到Driver端。
   * 2.在Driver程序中定义的变量，在Executor端的每个Task都会得到这个变量的一份新的副本，每个task更新这些副本的值后，传回Driver端进行merge。
   * 3.少加：累加器需要action使用action算子才执行，单独transform算子不会执行生效
   * 4.多加：如果执行多次action算子 会执行多次，累加多次
   * 5.一般情况下，累加器回在action算子中执行
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    // TODO: 2.业务逻辑
    // TODO: 2.1 错误示例 累计器出现的原因：算子内变量由Executor执行，计算完成后不会回传至Driver
    //accumulatorError(sc)
    // TODO: 2.2 算子内变量由Executor执行，计算完成后不会回传至Driver
    // TODO: Spark 默认提供了简单数据聚合的累加器
    accumulator(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * 累计器出现的原因：算子内变量由Executor执行，计算完成后不会回传至Driver
   * *此方法为错误示例
   * @param sc
   */
  def accumulatorError(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    var sum: Int = 0
    // TODO: foreach算子内部代码由Executor执行，每个executor内部的sum进行累加，但不回回传给driver，所以结果为0
    dataRDD.foreach(num => {
      sum += num
    })
    println(sum)
  }

  /**
   * spark 累加器
   * @param sc
   */
  def accumulator(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    // TODO: 声明、使用累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")
    dataRDD.foreach(num => {
      sum.add(num)
    })
    println(sum.value)
  }
}
