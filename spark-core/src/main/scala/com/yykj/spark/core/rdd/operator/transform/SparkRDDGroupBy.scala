package com.yykj.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.{Date, SimpleTimeZone}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDGroupBy {

  /**
   * groupBy:标准操作[Transform & Value单值类型]
   * 将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，我们将这样的操作称之为shuffle。极限情况下，数据可能被分在同一个分区中
   * 分组与分区没有必然联系
   * def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.执行操作
    // TODO: 2.1 groupBy标准操作：将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，我们将这样的操作称之为shuffle
    transformGroupBy(sc)
    // TODO: 2.2 groupBy根据首写字母进行分组
    //transformGroupByFirstWord(sc)
    // TODO: 2.3 groupBy分析日志(建议使用Reduce进行数据聚合)
    //transformGroupByLog(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * groupBy标准操作
   * @param sc
   */
  def transformGroupBy(sc: SparkContext): Unit = {
    val dataRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4),2)

    // TODO: 1.标准写法
      // 1.1获取数据对应的组编码方法
//    def getGroupCode(num: Int): Int = {
//      num % 2
//    }
//
//    // 1.2 返回值 RDD[(K, Iterable[T])] k:分组编码 Iterable[T]:分组数据
//    val groupRDD: RDD[(Int, Iterable[Int])] = dataRDD.groupBy(getGroupCode)
//
//    // 1.3 输出
//    groupRDD.collect().foreach(tup => {
//      println("Group=" + tup._1 + " Data=" + tup._2)
//    })

    // TODO: 2.简化写法
    // (1,CompactBuffer(1, 3))
    // (0,CompactBuffer(2, 4))
    val groupRDD: RDD[(Int, Iterable[Int])] = dataRDD.groupBy(_ % 2)
    groupRDD.foreach(println)
  }

  /**
   * groupBy根据手首写字母进行分组
   * @param sc
   */
  def transformGroupByFirstWord(sc: SparkContext): Unit = {
    val dataRDD: RDD[String] = sc.parallelize(List("Hello", "hive", "hbase", "Hadoop"),2)
    val groupRDD: RDD[(String, Iterable[String])] = dataRDD.groupBy(_.take(1))
    groupRDD.collect().foreach(println)
  }

  /**
   * groupBy分析日志(建议使用Reduce进行数据聚合)
   * @param sc
   */
  def transformGroupByLog(sc: SparkContext): Unit = {

    // TODO: 1.数据RDD
    val dataRDD: RDD[String] = sc.textFile("spark-core/datas/input/apache.log",2)

    // TODO: 2.Map算子 每行数据根据小时生成元组(hour, 1)
    val mapRDD: RDD[(String, Int)] = dataRDD.map(line => {
      val datas: Array[String] = line.split(" ")
      val format_1 = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val date: Date = format_1.parse(datas(3))
      val format_2 = new SimpleDateFormat("HH")
      val hour = format_2.format(date)
      (hour, 1)
    })

    // TODO: 3.groupby算子 按小时分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupBy(_._1)

    // TODO: 4.map算子 根据分组key（小时）生成数据条数
    val mapRDD_2: RDD[(String, Int)] = groupRDD.map(tuple => {
      (tuple._1, tuple._2.size)
    })

    // TODO: 4.sortbykey 排序
    val sortRDD = mapRDD_2.sortByKey(true)

    // TODO: 5.输出
    sortRDD.collect().foreach(println)

  }
}
