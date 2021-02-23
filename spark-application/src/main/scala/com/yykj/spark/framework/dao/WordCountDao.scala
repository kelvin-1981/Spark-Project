package com.yykj.spark.framework.dao

import com.yykj.spark.framework.common.TDao
import com.yykj.spark.framework.util.EnvUtils
import org.apache.spark.rdd.RDD

class WordCountDao extends TDao{

  override def readFile(path: String): RDD[String] = {
    EnvUtils.get().textFile(path)
  }

  override def readHdfs(path: String): Any = ???

  override def readMySQL(server: String, port: String, user: String, pwd: String): Any = ???

  override def readOracle(server: String, port: String, user: String, pwd: String): Any = ???
}
