package com.yykj.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDForeach {

  /**
   * foreach:标准操作[Action] Action算子：出发作业（Job）执行的算子
   * 分布式遍历RDD中的每一个元素，调用指定函数
   * def foreach(f: T => Unit): Unit = withScope {
   *  val cleanF = sc.clean(f)
   *  sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
   * }
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 foreach:分布式遍历RDD中的每一个元素，调用指定函数


    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * foreach:分布式遍历RDD中的每一个元素，调用指定函数
   * @param sc
   */
  def actionForeach(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val res: Int = dataRDD.aggregate(0)(_ + _, _ * _)
    println(res)
  }
}
