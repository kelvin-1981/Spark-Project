package com.yykj.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {

  def main(args: Array[String]): Unit = {

    // TODO: 声明对象
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)
    println("start...")

    for(i <- 1 to 100) {
      println(i)
    }

    // TODO: 执行业务操作
    // 1:hello world 2:hello spark
    val rdd_line: RDD[String] = sc.textFile("datas/word-data")

    // 1.hello 2:world
    val rdd_words: RDD[String] = rdd_line.flatMap(_.split(" "))

    // 1.(hello,hello,hello) 2.(world) 3.(spark)
    val rdd_tuple: RDD[(String, Int)] = rdd_words.map((_, 1))

    // hello:3 world:1
    val rdd_group: RDD[(String, Int)] = rdd_tuple.reduceByKey(_ + _)

    // 排序结果
    val rdd_sort: RDD[(String, Int)] = rdd_group.sortBy(_._2, false)

    // 打印结果
    println(rdd_sort.collect().toString);

    // TODO: 关闭
    sc.stop()
  }
}
