package com.yykj.spark.framework.service

import com.yykj.spark.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * 服务层
 */
class WordCountService {

  // TODO: 持久层对象
  private var dao = new WordCountDao()

  /**
   * 数据分析
   */
  def dataAnalysis() = {

    // TODO: 调用持久层对象 读取数据文件
    val rdd_line = dao.readDataFile("spark-application/datas/input/word-data")

    // 1.hello 2:world
    val wordRDD: RDD[String] = rdd_line.flatMap(_.split(" "))
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
