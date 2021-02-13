package com.yykj.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDGroupByKey {

  /**
   * groupByKey:标准操作[Transform & Value单值类型] : 进行分区数量的重新设置(缩小或扩大分区数量)
   * 将数据源的数据根据key对value进行分组
   * def groupByKey(): RDD[(K, Iterable[V])]
   * def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])]
   * def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.逻辑计算
    // TODO: 2.1 groupByKey:将数据源的数据根据key对value进行分组
    transformGroupByKey(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * groupByKey:将数据源的数据根据key对value进行分组
   * @param sc
   */
  def transformGroupByKey(sc: SparkContext): Unit = {
    val dataRDD = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))
    // TODO: 相同key的数据分在一个组中，形成一个对偶元组(String, Iterable[T])
    val groupRDD: RDD[(String, Iterable[Int])] = dataRDD.groupByKey()
    groupRDD.collect().foreach(println)
  }
}
