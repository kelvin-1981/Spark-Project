package com.yykj.spark.framework.dao

import org.apache.spark.rdd.RDD

/**
 * 持久层
 */
class WordCountDao  {

  def readDataFile(path: String) = {
    // 1:hello world 2:hello spark
    val rdd_line: RDD[String] = sc.textFile(path)
    rdd_line
  }
}
