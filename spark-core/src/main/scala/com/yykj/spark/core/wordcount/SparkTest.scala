package com.yykj.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {

  /**
   * 
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.声明
    val conf = new SparkConf().setMaster("local").setAppName("wc")
    val sc = new SparkContext(conf)

    // TODO: 2.读取文件
    val lines: RDD[String] = sc.textFile("spark-core/datas/input/word-data")

    // TODO: 3.扁平化
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //println(words.collect().toList)

    // TODO: 4.map遍历生成元组
    val tuples: RDD[(String, Int)] = words.map((_,1))
    //val tuples: RDD[(String, Int)] = words.map(word => (word, 1))

    // TODO: 5.reduce 分组、计算
    val values: RDD[(String, Int)] = tuples.reduceByKey(_ + _)
    //val values: RDD[(String, Int)] = tuples.reduceByKey((x, y) => x + y)

    // TODO: 6.排序
    val res: RDD[(String, Int)] = values.sortBy(_._2, false)

    // TODO: 7.输出数据至文件
    val res_2: Array[(String, Int)] = res.collect()
    res_2.foreach(println)

    //res.saveAsTextFile(args(1))

    // TODO: 8.关闭
    sc.stop()

    var i = 5 / 2;
    var k = 1 % 2;
    println(k)
  }
}
