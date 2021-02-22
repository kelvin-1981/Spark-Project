package com.yykj.spark.core.samples.ec

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object HotCategoryTOP10Analysis {

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
    //HotCategoryAnalysis(sc)
    // TODO: 2.2.2 按点击量、下单量、支付量分别统计 & 直接转换最终格式
    //HotCategoryAnalysis02(sc)
    // TODO: 2.2.3 直接转换最终格式 & 数据RDD共用
    //HotCategoryAnalysis03(sc)
    // TODO: 2.2.4 累加器实现计算 不含shuffle
    HotCategoryAnalysis04(sc)

    // TODO: 2.3 环境关闭
    sc.stop()
  }

  /**
   * 按点击量、下单量、支付量分别统计
   * @param sc
   */
  def HotCategoryAnalysis(sc: SparkContext): Unit = {
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

    // TODO: 6. 将数据转换格式
    val anayRDD: RDD[(String, (Int, Int, Int))] = cognRDD.map {
      case (cate, tup) => (cate, (tup._1.iterator.next(), tup._2.iterator.next(), tup._3.iterator.next()))
    }

    // TODO: 7.排序
    val sortRDD: RDD[(String, (Int, Int, Int))] = anayRDD.sortBy(_._2, false)

    // TODO: 8.TOP 10
    val resArr: Array[(String, (Int, Int, Int))] = sortRDD.take(10)

    // TODO: 9.输出
    resArr.foreach(println)

    // TODO: 10 计算结果
    // (2,(6119,1767,1196))
    // (20,(6098,1776,1244))
    // (12,(6095,1740,1218))
    // (11,(6093,1781,1202))
    // (17,(6079,1752,1231))
    // (7,(6074,1796,1252))
    // (9,(6045,1736,1230))
    // (19,(6044,1722,1158))
    // (13,(6036,1781,1161))
  }

  /**
   * 按点击量、下单量、支付量分别统计
   * @param sc
   */
  def HotCategoryAnalysis02(sc: SparkContext): Unit = {
    // TODO: 1. 读取数据
    val dataRDD: RDD[String] = sc.textFile("spark-core/datas/input/sample/user_visit_action.txt")

    // TODO: 2.点击量统计 判断条件：如果点击的品类ID和产品ID为-1，表示数据不是点击数据
    val cFilterRDD = dataRDD.filter(_.split("_")(6) != "-1")
    val cMapRDD: RDD[(String, (Int, Int, Int))] = cFilterRDD.map(line => {
      val arr = line.split("_")
      (arr(6), (1, 0, 0))
    })

    // TODO: 3.下单量统计
    //  A.针对于下单行为一次可以下单多个商品，所以品类ID和产品ID可以是多个，id之间采用逗号分隔
    //  B如果本次不是下单行为，则数据采用 null 表示
    val oFilterRDD: RDD[String] = dataRDD.filter(_.split("_")(8) != "null")
    val oMapRDD: RDD[(String, (Int, Int, Int))] = oFilterRDD.flatMap(line => {
      val arr = line.split("_")
      val cateIdArr = arr(8).split(",")
      cateIdArr.map((_, (0, 1, 0)))
    })

    // TODO: 4.下单量统计 判断条件：如果本次不是支付行为，则数据采用 null 表示 arr(10)
    val pFilterRDD: RDD[String] = dataRDD.filter(_.split("_")(10) != "null")
    val pMapRDD: RDD[(String, (Int, Int, Int))] = pFilterRDD.flatMap(line => {
      val arr = line.split("_")
      val cateIdArr = arr(10).split(",")
      cateIdArr.map((_, (0, 0, 1)))
    })

    // TODO: 5. 关联RDD
    val unionRDD: RDD[(String, (Int, Int, Int))] = cMapRDD.union(oMapRDD).union(pMapRDD)

    // TODO: 6. 计算
    val reduceRDD: RDD[(String, (Int, Int, Int))] = unionRDD.reduceByKey {
      case (tup1, tup2) => {
        (tup1._1 + tup2._1, tup1._2 + tup2._2, tup1._3 + tup2._3)
      }
    }

    // TODO: 7.排序
    val sortRDD: RDD[(String, (Int, Int, Int))] = reduceRDD.sortBy(_._2, false)

    // TODO: 8.TOP 10
    val resArr: Array[(String, (Int, Int, Int))] = sortRDD.take(10)

    // TODO: 9.输出
    resArr.foreach(println)

    // TODO: 10 计算结果
    // (2,(6119,1767,1196))
    // (20,(6098,1776,1244))
    // (12,(6095,1740,1218))
    // (11,(6093,1781,1202))
    // (17,(6079,1752,1231))
    // (7,(6074,1796,1252))
    // (9,(6045,1736,1230))
    // (19,(6044,1722,1158))
    // (13,(6036,1781,1161))
  }

  /**
   * 同时统计点击量、下单量、支付量
   * @param sc
   */
  def HotCategoryAnalysis03(sc: SparkContext): Unit = {
    // TODO: 1. 读取数据
    val dataRDD: RDD[String] = sc.textFile("spark-core/datas/input/sample/user_visit_action.txt")
    dataRDD.cache()

    // TODO: 2.转换结构 直接构建(品类ID,(点击量,下单量,支付量))
    val mapRDD: RDD[(String, (Int, Int,Int))] = dataRDD.flatMap(line => {
      var arr: Array[String] = line.split("_")
      var structMap = mutable.Map[String, (Int, Int,Int)]()
      // 点击量
      if (arr(6) != "-1") {
        structMap.update(arr(6), (1, 0, 0))
      }
      // 下单量
      if (arr(8) != "null") {
        val codes: Array[String] = arr(8).split(",")
        codes.map(structMap.update(_, (0, 1, 0)))
      }
      // 支付量
      if (arr(10) != "null") {
        val codes: Array[String] = arr(10).split(",")
        codes.map(structMap.update(_, (0, 0, 1)))
      }

      structMap
    })

    // TODO: 3.计算汇总值
    val reduceRDD: RDD[(String, (Int, Int, Int))] = mapRDD.reduceByKey {
      case (tup1, tup2) => {
        (tup1._1 + tup2._1, tup1._2 + tup2._2, tup1._3 + tup2._3)
      }
    }

    // TODO: 4.排序
    val sortRDD = reduceRDD.sortBy(_._2, false)

    // TODO: 5.TOP 10
    val resArr: Array[(String, (Int, Int, Int))] = sortRDD.take(10)

    // TODO: 6.输出
    resArr.foreach(println)
  }

  /**
   * 使用累加器实现 减少shuffle
   * @param sc
   */
  def HotCategoryAnalysis04(sc: SparkContext): Unit = {
    // TODO: 1. 读取数据
    val dataRDD: RDD[String] = sc.textFile("spark-core/datas/input/sample/user_visit_action.txt")
    dataRDD.cache()

    // TODO: 2.创建、注册自定义累加器
    val acc = new HotCateAccumulator()
    sc.register(acc,"EC")

    // TODO: 3.自定义累加器实现计算 不含shuffle
    dataRDD.foreach(line => {
      var arr: Array[String] = line.split("_")
      // 点击量
      if (arr(6) != "-1") {
        acc.add((arr(6),"C"))
      }
      // 下单量
      if (arr(8) != "null") {
        val codes: Array[String] = arr(8).split(",")
        codes.foreach(action => {
          acc.add((action,"O"))
        })
      }
      // 支付量
      if (arr(10) != "null") {
        val codes: Array[String] = arr(10).split(",")
        codes.foreach(action => {
          acc.add((action,"P"))
        })
      }
    })

    // TODO: 4.读取累加器数值
    val mapArr: mutable.Map[String, HotCategory] = acc.value

    // TODO: 5.转换数据结构 以备排序
    val mapArr_2: mutable.Map[String, (Int, Int, Int)] = mapArr.map {
      case (code, info) => {
        (code, (info.cCount, info.oCount, info.pCount))
      }
    }

    // TODO: 6.排序、TOP10
    val resArr: List[(String, (Int, Int, Int))] = mapArr_2.toList.sortBy(_._2)(Ordering.Tuple3[Int,Int,Int].reverse).take(10)

    // TODO: 7.输出
    resArr.foreach(println)
  }

  /**
   * 创建对象类
   */
  case class HotCategory(code: String,var cCount: Int, var oCount: Int, var pCount: Int)

  /**
   * 自定义累加器
   */
  class HotCateAccumulator extends AccumulatorV2[(String,String),mutable.Map[String,HotCategory]]{

    private var ecMap = mutable.Map[String,HotCategory]()

    override def isZero: Boolean = {
      ecMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCateAccumulator()
    }

    override def reset(): Unit = {
      ecMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      var code: String = v._1
      var action: String = v._2
      val info: HotCategory = ecMap.getOrElse(code, HotCategory(code, 0, 0, 0))
      action match{
        case "C" => info.cCount += 1
        case "O" => info.oCount += 1
        case "P" => info.pCount += 1
      }
      ecMap.update(code, info)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map_1: mutable.Map[String, HotCategory] = this.ecMap
      val map_2: mutable.Map[String, HotCategory] = other.value
      map_2.foreach{
        case (code, info_2) => {
          val info: HotCategory = map_1.getOrElse(code, HotCategory(code, 0, 0, 0))
          info.cCount += info_2.cCount
          info.oCount += info_2.oCount
          info.pCount += info_2.pCount
          map_1.update(code, info)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = {
      ecMap
    }
  }
}
