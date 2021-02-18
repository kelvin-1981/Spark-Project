package com.yykj.spark.core.acc

import scala.collection.mutable
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, CollectionAccumulator, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

object SparkAccumulatorWordCount {

  /**
   * 1.累加器实现wordcount
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    // TODO: 2.业务逻辑
    // TODO: 2.1 数据预处理
    val lines: RDD[String] = sc.makeRDD(List("hello spark","hello scala"))
    val flatRDD: RDD[String] = lines.flatMap(_.split(" "))
    // TODO: 2.2 声明、注册自定义累加器
    val acc: myAccumulator = new myAccumulator
    sc.register(acc, "WC")
    //此种方式减少了shuffle
    flatRDD.foreach(word => {
      acc.add(word)
    })
    val wcMap: mutable.Map[String, Int] = acc.value

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * 自定义累加器
   * 1.继承AccumulatorV2
   * 2.定义泛型 IN：累加器输入数据类型 OUT：累加器输出返回类型
   */
  class myAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{

    private var wcMap = mutable.Map[String,Int]()

    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      new myAccumulator
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    override def add(word: String): Unit = {
      val num = wcMap.getOrElse(word, 0) + 1
      wcMap.update(word, num)
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      val map_1 = this.wcMap
      val map_2 = other.value

      map_2.foreach{
        case (word, count) => {
          val num = map_1.getOrElse(word, 0) + count
          map_1.update(word, num)
        }
      }

    }

    override def value: mutable.Map[String, Int] = {
      wcMap
    }
  }
}
