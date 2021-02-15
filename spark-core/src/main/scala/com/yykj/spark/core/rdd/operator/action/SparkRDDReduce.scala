package com.yykj.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDReduce {
  /**
   * reduce:标准操作[Action] Action算子：出发作业（Job）执行的算子
   * 聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据
   * def reduce(f: (T, T) => T): T
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 reduce:聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据
    actionReduce(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * collect:聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据
   * @param sc
   */
  def actionReduce(sc: SparkContext): Unit = {
    val dataRDD = sc.makeRDD(List(1, 2, 3, 4, 5))
    val res: Int = dataRDD.reduce(_ + _)
    println(res)
  }
}
