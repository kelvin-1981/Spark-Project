package com.yykj.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDCollect {
  /**
   * collect:标准操作[Action] Action算子：出发作业（Job）执行的算子
   * 将不同分区的数据按分区顺序采集到Driver端内存数组，以数组Array的形式返回数据集的所有元素
   * def collect(): Array[T]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 collect:将不同分区的数据按分区顺序采集到Driver端内存数组，以数组Array的形式返回数据集的所有元素
    actionCollect(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * collect:聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据
   * @param sc
   */
  def actionCollect(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5),2)
    val resArr: Array[Int] = dataRDD.collect()
    resArr.foreach(println)
  }
}
