package com.yykj.spark.core.samples.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkWorCount02 {

  /**
   * 可实现WordCount的算子介绍-扩展
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.环境准备
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    // TODO: 2.执行业务操作
    // TODO: 2.1 标准方法:reduceByKey
    //wordCountReduceByKey(sc)
    // TODO: 2.2 实现方法:groupBy
    //wordCountGroupBy(sc)
    // TODO: 2.3 实现方法:groupByKey
    //wordCountGroupByKey(sc)
    // TODO: 2.4 实现方法：aggregateByKey
    //wordCountAggregateByKey(sc)
    // TODO: 2.5 实现方法：fold
    //wordCountFold(sc)
    // TODO: 2.6 实现方法：countByKey
    //wordCountCountByKey(sc)
    // TODO: 2.7 实现方法：countByKey
    wordCountCountByValue(sc)
    // TODO: 2.8 实现方法：combineByKey
    //wordCountCombineByKey(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * 实现方法：reduceByKey
   * @param sc
   */
  def wordCountReduceByKey(sc: SparkContext): Unit = {
    // 1:hello world 2:hello spark
    val rdd_line: RDD[String] = sc.textFile("spark-core/datas/input/word-data")
    // 1.hello 2:world
    val wordRDD: RDD[String] = rdd_line.flatMap(_.split(" "))
    // 1.(hello,hello,hello) 2.(world) 3.(spark)
    val mapRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    // hello:3 world:1
    val redRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    // 排序结果
    val sortRDD: RDD[(String, Int)] = redRDD.sortByKey(true)
    // 打印结果
    sortRDD.collect().foreach(println)
  }

  /**
   * 实现方法:groupBy
   * @param sc
   */
  def wordCountGroupBy(sc: SparkContext): Unit = {
    val dataRDD: RDD[String] = sc.makeRDD(List("hello scala", "hello spark", "hello hadoop"))
    val mapRDD: RDD[String] = dataRDD.flatMap(_.split(" "))
    val groupRDD: RDD[(String, Iterable[String])] = mapRDD.groupBy(word => word)
    val mapRDD2: RDD[(String, Int)] = groupRDD.mapValues(_.size)
    mapRDD2.collect().foreach(println)
  }

  /**
   * 实现方法:groupByKey
   * @param sc
   */
  def wordCountGroupByKey(sc: SparkContext): Unit = {
    val dataRDD: RDD[String] = sc.makeRDD(List("hello scala", "hello spark", "hello hadoop"))
    val mapRDD: RDD[String] = dataRDD.flatMap(_.split(" "))
    val mapRDD_2: RDD[(String, Int)] = mapRDD.map((_, 1))
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD_2.groupByKey()
    val mapRDD_3: RDD[(String, Int)] = groupRDD.mapValues(_.size)
    mapRDD_3.collect().foreach(println)
  }

  /**
   * 实现方法:aggregateByKey
   * @param sc
   */
  def wordCountAggregateByKey(sc: SparkContext): Unit = {
    val dataRDD: RDD[String] = sc.makeRDD(List("hello scala", "hello spark", "hello hadoop"))
    val mapRDD: RDD[String] = dataRDD.flatMap(_.split(" "))
    val mapRDD_2: RDD[(String, Int)] = mapRDD.map((_, 1))
    val aggRDD: RDD[(String, Int)] = mapRDD_2.aggregateByKey(0)(_ + _, _ + _)
    aggRDD.collect().foreach(println)
  }

  /**
   * 实现方法:foldByKey
   * @param sc
   */
  def wordCountFold(sc: SparkContext): Unit = {
    val dataRDD: RDD[String] = sc.makeRDD(List("hello scala", "hello spark", "hello hadoop"))
    val mapRDD: RDD[String] = dataRDD.flatMap(_.split(" "))
    val mapRDD_2: RDD[(String, Int)] = mapRDD.map((_, 1))
    val foldRDD: RDD[(String, Int)] = mapRDD_2.foldByKey(0)(_ + _)
    foldRDD.collect().foreach(println)
  }

  /**
   * 实现方法:countByKey
   * @param sc
   */
  def wordCountCountByKey(sc: SparkContext): Unit = {
    val dataRDD: RDD[String] = sc.makeRDD(List("hello scala", "hello spark", "hello hadoop"))
    val mapRDD: RDD[String] = dataRDD.flatMap(_.split(" "))
    val mapRDD_2: RDD[(String, Int)] = mapRDD.map((_, 1))
    val countArr: collection.Map[String, Long] = mapRDD_2.countByKey()
    countArr.foreach(println)
  }

  /**
   * 实现方法:countByValue
   * @param sc
   */
  def wordCountCountByValue(sc: SparkContext): Unit = {
    val dataRDD: RDD[String] = sc.makeRDD(List("hello scala", "hello spark", "hello hadoop"))
    val mapRDD: RDD[String] = dataRDD.flatMap(_.split(" "))
    val resArr: collection.Map[String, Long] = mapRDD.countByValue()
    resArr.foreach(println)
  }

  /**
   * 实现方法:combineByKey
   * @param sc
   */
  def wordCountCombineByKey(sc: SparkContext): Unit = {
    val dataRDD: RDD[String] = sc.makeRDD(List("hello scala", "hello spark", "hello hadoop"))
    val mapRDD: RDD[String] = dataRDD.flatMap(_.split(" "))
    val mapRDD_2: RDD[(String, Int)] = mapRDD.map((_, 1))
    val comRDD: RDD[(String, Int)] = mapRDD_2.combineByKey(
      v => v,
      (x1: Int, x2: Int) => x1 + x2,
      (x1: Int, x2: Int) => x1 + x2
    )
    comRDD.collect().foreach(println)
  }
}
