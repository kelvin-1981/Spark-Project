package com.yykj.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDGroupByKey {

  /**
   * groupByKey:标准操作[Transform & Key-Value类型 & shuffle算子]
   * 将数据源的数据根据key对value进行分组
   *
   * ***：Spark Shuffle必须进行落盘处理，不能在内存中等待，会导致内存溢出。Shuffle性能低
   * ***：reduceByKey：分区内与分区间的计算规则一致，如需不一致需要使用aggregateByKey算子
   * ***：reduceByKey和groupByKey的核心区别：
   * 1）shuffle区别：reduceByKey和groupByKey都存在shuffle的操作，但是reduceByKey可以在shuffle前对分区内相同key的数据进行预聚合（combine）功能，
   * 这样会减少落盘的数据量，而groupByKey只是进行分组，不存在数据量减少的问题，reduceByKey性能比较高。
   * 2）功能区别：reduceByKey其实包含分组和聚合的功能。GroupByKey只能分组，不能聚合，所以在分组聚合的场合下，
   * 推荐使用reduceByKey，如果仅仅是分组而不需要聚合。那么还是只能使用groupByKey
   *
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
    // TODO: 计算结果
    // (a,CompactBuffer(1, 2, 3))
    // (b,CompactBuffer(4))
    groupRDD.collect().foreach(println)
  }
}
