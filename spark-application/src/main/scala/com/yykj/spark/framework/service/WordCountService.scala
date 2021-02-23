package com.yykj.spark.framework.service

import com.yykj.spark.framework.common.TService
import com.yykj.spark.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

class WordCountService extends TService{

  private var dao = new WordCountDao()

  override def dataAnalysis(): Array[(String, Int)] = {
    val dataRDD: RDD[String] = dao.readFile("spark-application/datas/input/word-data")
    // 1.hello 2:world
    val wordRDD: RDD[String] = dataRDD.flatMap(_.split(" "))
    // 1.(hello,hello,hello) 2.(world) 3.(spark)
    val mapRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    // hello:3 world:1
    val redRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    // 排序结果
    val sortRDD: RDD[(String, Int)] = redRDD.sortByKey(true)
    // 收集结果
    val resArr = sortRDD.collect()
    resArr
  }
}
