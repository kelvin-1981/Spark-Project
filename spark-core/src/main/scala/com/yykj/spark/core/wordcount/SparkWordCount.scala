package com.yykj.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 声明对象
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)
    println("start...")

    // TODO: 执行业务操作
    // 1:hello world 2:hello spark
    val rdd_line: RDD[String] = sc.textFile("spark-core/datas/input/word-data")

    // 1.hello 2:world
    val rdd_words: RDD[String] = rdd_line.flatMap(_.split(" "))
    //for (item <- rdd_words.collect()){
    //  println(item)
    //}

    // 1.(hello,hello,hello) 2.(world) 3.(spark)
    val rdd_tuple: RDD[(String, Int)] = rdd_words.map((_, 1))
    //for (item <- rdd_tuple.collect()){
    //  println(item._1 + " " + item._2)
    //}

    // hello:3 world:1
    val rdd_group: RDD[(String, Int)] = rdd_tuple.reduceByKey(_ + _)
    //val rdd_group: RDD[(String, Int)] = rdd_tuple.reduceByKey(sum _)

    // 排序结果
    val rdd_sort: RDD[(String, Int)] = rdd_group.sortBy(_._2, false)

    // 打印结果
    for (item <- rdd_sort.collect()){
      //println(item)
      println(item._1 + ":" + item._2)
    }

    // TODO: 关闭
    sc.stop()
  }

  /**
   * 自定义相加方法
   * @param v1
   * @param v2
   * @return
   */
  def sum(v1 : Int, v2 : Int) : Int = {
    v1 + v2
  }
}
