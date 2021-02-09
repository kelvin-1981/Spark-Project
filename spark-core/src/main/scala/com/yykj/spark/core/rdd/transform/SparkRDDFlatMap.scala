package com.yykj.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDFlatMap {

  /**
   * flatMap:标准操作[Transform & Value单值类型]
   * 将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射
   * def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U]
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 注意：flatmap算子执行 第一步：map 第二步：flat
    // 例：["a b c", "", "d"] => [["a","b","c"],[],["d"]] => ["a","b","c","d"]
    // 0) val dataRDD: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))
    // 0) val flatRDD: RDD[Int] = dataRDD.flatMap(_.map(_ * 10))
    // 1) 执行map Array(List(1, 2),List(3, 4))
    // 2) 执行    f:function匿名函数 => List(10, 20),List(30, 40)
    // 3）执行flat


    // TODO: 2.执行操作
    // TODO: 2.1 flatMap标准操作 含简化写法 扁平化处理，将集合中的元素（元素类型int、string、list不同处理逻辑不同）返回，后续进行逻辑操作
    //transformFlatMap(sc)
    // TODO: 2.2 flatMap进行数据切分
    //transformFlatMapSplit(sc)
    // TODO: 2.3 flatMap获取不同类型数据的处理
    //transformFlatMapType(sc)

    // TODO: 3.map与flatMap算子区别 执行步骤及解释
    // 0) val dataRDD: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))
    // 0) val flatRDD: RDD[Int] = dataRDD.flatMap(_.map(_ * 10))
    // 1) 执行map第一步: List(1, 2) List(3, 4)
    // 2) 执行map第二步: f:function匿名函数 => List(10, 20) List(30, 40)
    // 3）执行flat:     10 20 30 40
    transforMapAndFlatMap(sc)

    // TODO: 4.关闭环境
    sc.stop()
  }

  /**
   * flatMap标准操作[Value集合类型]
   * @param sc
   */
  def transformFlatMap(sc: SparkContext): Unit = {
    val dataRDD: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))

//    val flatRDD: RDD[Int] = dataRDD.flatMap(iter => {
//      println(iter)
//      iter.map(_ * 10)
//    })
    val flatRDD: RDD[Int] = dataRDD.flatMap(_.map(_ * 10))
    flatRDD.collect.foreach(println)
  }

  /**
   * flatMap进行数据读取切分
   * @param sc
   */
  def transformFlatMapSplit(sc: SparkContext): Unit = {
    val dataRDD: RDD[String] = sc.makeRDD(List("hello scala", "hello spark"))
    val flatRDD: RDD[String] = dataRDD.flatMap(_.split(" "))
    flatRDD.collect.foreach(println)
  }

  /**
   * flatMap标准操作进行不同类型的元素处理
   * @param sc
   */
  def transformFlatMapType(sc: SparkContext): Unit = {

    // TODO: 元素类型不同 List：List(1,2) Int:3 List:List(4,5) *dataRDD: RDD[Any] Spark无法判断元素类型
    val dataRDD: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))

    // TODO: 当前处理flatmap返回元素为List(1, 2) List(3) List(4, 5)
    val flatRDD: RDD[Any]  = dataRDD.flatMap(data => {
      println("source: " + data)
      data match {
        case list: List[_] => {println(list); list}
        case value: Int => {println(List(value)); List(value)}
      }
    })

    flatRDD.collect.foreach(println)
  }

  /**
   * 比较map算组与flatmap算子区别
   * @param sc
   */
  def transforMapAndFlatMap(sc: SparkContext): Unit = {
    // TODO: 执行步骤及解释
    // 0) val dataRDD: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))
    // 0) val flatRDD: RDD[Int] = dataRDD.flatMap(_.map(_ * 10))
    // 1) 执行map第一步: List(1, 2) List(3, 4)
    // 2) 执行map第二步: f:function匿名函数 => List(10, 20) List(30, 40)
    // 3）执行flat:     10 20 30 40

    val dataRDD: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))

    val flatRDD: RDD[Int] = dataRDD.flatMap(_.map(_ * 10))
    flatRDD.collect.foreach(println)

    println("---------------------------------------------")
    val mapRDD: RDD[List[Int]] = dataRDD.map(_.map(_ * 10))
    mapRDD.collect().foreach(println)
  }
}
