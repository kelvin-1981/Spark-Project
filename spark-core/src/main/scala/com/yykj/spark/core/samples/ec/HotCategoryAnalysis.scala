package com.yykj.spark.core.samples.ec

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HotCategoryAnalysis {

  /**
   * 电商案例-需求1：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.数据说明
    /**
     * 上面的数据图是从数据文件中截取的一部分内容，表示为电商网站的用户行为数据，主
     * 要包含用户的 4 种行为： 搜索，点击，下单，支付 。 数据规则如下：
     * ➢ 数据文件中每行数据采用_分隔数据
     * ➢ 每一行数据表示用户的一次行为，这个行为只能是 4 种行为的一种
     * ➢ 如果搜索关键字为 null, 表示数据不是搜索数据
     * ➢ 如果点击的品类ID和产品ID为-1，表示数据不是点击数据
     * ➢ 针对于下单行为一次可以下单多个商品，所以品类ID和产品ID可以是多个，id之间采用逗号分隔，如果本次不是下单行为，则数据采用 null 表示
     * ➢ 支付行为和下单行为类似
     */

    // TODO: 2.开发

    // TODO: 2.1 环境准备
    val conf = new SparkConf().setMaster("local[*]").setAppName("SC")
    val sc = new SparkContext(conf)

    // TODO: 2.2 业务逻辑
    // TODO: 2.2.1 按点击量、下单量、支付量分别统计
    HotCategoryDetailsAnalysis(sc)

    // TODO: 2.3 环境关闭
    sc.stop()
  }

  /**
   * 按点击量、下单量、支付量分别统计
   * @param sc
   */
  def HotCategoryDetailsAnalysis(sc: SparkContext): Unit = {
    // TODO: 1. 读取数据
    val dataRDD: RDD[String] = sc.textFile("spark-core/datas/input/sample/user_visit_action.txt")

    // TODO: 2.点击量统计 判断条件：如果点击的品类ID和产品ID为-1，表示数据不是点击数据
    val cFilterRDD = dataRDD.filter(_.split("_")(6) != "-1")
    val cMapRDD = cFilterRDD.map(line => {
      val arr = line.split("_")
      ((arr(6), 1))
    })
    val cReduceRDD: RDD[(String, Int)] = cMapRDD.reduceByKey(_ + _)

    // TODO: 3.下单量统计
    //  A.针对于下单行为一次可以下单多个商品，所以品类ID和产品ID可以是多个，id之间采用逗号分隔
    //  B如果本次不是下单行为，则数据采用 null 表示
    val oFilterRDD: RDD[String] = dataRDD.filter(_.split("_")(8) != "null")
    val oMapRDD: RDD[(String, Int)] = oFilterRDD.flatMap(line => {
      val arr = line.split("_")
      val cateIdArr = arr(8).split(",")
      cateIdArr.map((_, 1))
    })
    val oReduceRDD: RDD[(String, Int)] = oMapRDD.reduceByKey(_ + _)

    // TODO: 4.下单量统计 判断条件：如果本次不是支付行为，则数据采用 null 表示 arr(10)
    val pFilterRDD: RDD[String] = dataRDD.filter(_.split("_")(10) != "null")
    val pMapRDD: RDD[(String, Int)] = pFilterRDD.flatMap(line => {
      val arr = line.split("_")
      val cateIdArr = arr(10).split(",")
      cateIdArr.map((_, 1))
    })
    val pReduceRDD: RDD[(String, Int)] = pMapRDD.reduceByKey(_ + _)

    // TODO: 5. 关联RDD
    val cognRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = cReduceRDD.cogroup(oReduceRDD, pReduceRDD)

    // TODO: 6.排序
    val sortRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = cognRDD.sortBy(_._2,false)

    // TODO: 7.TOP 10
    val resArr: Array[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = sortRDD.take(10)

    // TODO: 8.输出
    resArr.foreach(println)
  }
}
