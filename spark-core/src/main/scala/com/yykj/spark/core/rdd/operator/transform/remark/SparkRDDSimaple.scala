package com.yykj.spark.core.rdd.operator.transform.remark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDSimaple {

  /**
   * 1) 数据准备
   * agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
   * 2) 需求描述
   * 统计出每一个省份每个广告被点击数量排行的Top3
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sample")
    val sc: SparkContext = new SparkContext(conf)

    // TODO: 2.业务逻辑
    // TODO: 2.1 读取日志文件 时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
    val dataRDD = sc.textFile("spark-core/datas/input/agent.log")
    // TODO: 2.2 数据格式转换 ((省份,广告),1)
    val mapRDD: RDD[((String, String), Int)] = dataRDD.map(line => {
      val dataArr = line.split(" ")
      (((dataArr(1), dataArr(4)), 1))
    })
    // TODO: 2.3 省份广告点击量求和 ((省份,广告),sum)
    val sumRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)
    // TODO: 2.4 数据格式转换 (省份,(广告,sum))
    val mapRDD2: RDD[(String, (String, Int))] = sumRDD.map(tup => {
      (tup._1._1, (tup._1._2, tup._2))
    })
    // TODO: 2.5 根据省份分组 省份,iter(广告,sum)
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD2.groupByKey()
    // TODO: 2.6 广告&点击量 升序排序&TOP3
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(iter => {
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    })
    // TODO: 2.7 输出
    resRDD.collect().foreach(println)

    // TODO: 3.环境关闭
    sc.stop()
  }
}
