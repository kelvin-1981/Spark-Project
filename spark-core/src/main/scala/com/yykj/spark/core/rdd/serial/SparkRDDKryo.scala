package com.yykj.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDKryo {

  /**
   * Java的序列化能够序列化任何的类。但是比较重（字节多），序列化后，对象的提交也比较大。
   * Spark出于性能的考虑，Spark2.0开始支持另外一种Kryo序列化机制。Kryo速度是Serializable的10倍。
   * 当RDD在Shuffle数据的时候，简单数据类型、数组和字符串类型已经在Spark内部使用Kryo来序列化。
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.构建环境
    val conf: SparkConf = new SparkConf().setAppName("serial").setMaster("local[*]").
      set("spark.serializer","org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses (Array(classOf[Searcher]))
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "he llo atguigu", "atguigu", "hahah"),2)
    val searcher = new Searcher("hello")
    val result: RDD[String] = searcher.getMatchedRDD1(rdd)
    result.collect.foreach(println)
  }
}

case class Searcher(val query: String) {
  def isMatch(s: String) = {
    s.contains(query)
  }

  def getMatchedRDD1(rdd: RDD[String]) = {
    rdd.filter(isMatch)
  }

  def getMatchedRDD2(rdd: RDD[String]) = {
    val q = query
    rdd.filter(_.contains(q))
  }
}
