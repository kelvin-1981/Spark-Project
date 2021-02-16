package com.yykj.spark.core.rdd.serial

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkRDDSerial {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // TODO: 2.执行操作
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark","hive", "sql"))
    val search = new Search("hello")
    //search.getMatch1(rdd).collect().foreach(println)
    search.getMatch2(rdd).collect().foreach(println)

    // TODO: 3.关闭环境
    sc.stop()
  }
}

class Search(query: String) extends Serializable {

  def isMatch(s: String): Boolean = {
    s.contains(this.query)
  }

  // 函数序列化案例
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  // 属性序列化案例
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains(this.query))
  }
}