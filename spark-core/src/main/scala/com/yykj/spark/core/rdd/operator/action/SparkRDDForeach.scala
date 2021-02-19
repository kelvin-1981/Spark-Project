package com.yykj.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDForeach {

  /**
   * foreach:标准操作[Action] Action算子：出发作业（Job）执行的算子
   * 分布式遍历RDD中的每一个元素，调用指定函数.
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
    // TODO: 2.1 foreach:分布式遍历RDD中的每一个元素，调用指定函数.
    //actionForeach(sc)
    // TODO: 2.2 foreach位于executor端执行，使用对象需要序列化
    actionForeach02(sc)

    // TODO: RDD的方法和Scala集合对象的方法区别
    // 1) 集合对象的方法都是在同一个节点的内存中完成的。
    // 2) RDD的方法可以将计算逻辑发送到Executor端（分布式节点）执行
    // 3) 为了区分不同的处理效果，所以将RDD的方法称之为算子。
    // 4) RDD的方法外部的操作都是在Driver端执行的，而方法内部的逻辑代码是在Executor端执行。

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * foreach:分布式遍历RDD中的每一个元素，调用指定函数
   * @param sc
   */
  def actionForeach(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // TODO: 此foreach driver端执行
    dataRDD.collect().foreach(println)
    println("---------------------------------")
    // TODO: 此foreach executor端执行 无打印顺序
    dataRDD.foreach(println)
  }

  /**
   * foreach:对象序列化
   * @param sc
   */
  def actionForeach02(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val userInfo: user = new user
    dataRDD.foreach(num => {
      println((userInfo.age + num))
    })
  }

  class user extends Serializable {
    var age: Int = 10
  }
}

