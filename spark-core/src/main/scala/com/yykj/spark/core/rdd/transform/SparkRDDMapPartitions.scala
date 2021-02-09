package com.yykj.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDMapPartitions {

  /**
   * mapPartitions[Transform & Value单值类型]
   * 将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处理，哪怕是过滤数据
   * def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U],preservesPartitioning: Boolean = false): RDD[U]
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO: 1.创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(conf)

    // TODO: 2.执行操作
    // TODO: 2.1 mapPartitions标准操作 简化写法
    transformMapPartitions(sc)

    // TODO: 2.2 mapPartitions标准操作
    //transformMapPartitions02(sc)

    // TODO: 2.3 mapPartitions获取分区最大值
    //transformMapPartitions03(sc)

    // TODO: 3.关闭环境
    sc.stop()
  }

  /**
   * mapPartitions标准操作[Value集合类型]
   * @param sc
   */
  def transformMapPartitions(sc: SparkContext): Unit = {
    // TODO: 2.1 mapPartitions标准操作[Value集合类型]：读取单个分区的全部数据后进行逻辑处理
    // 1）以分区为单位进行数据转换操作 会将整体分区数据加载至内存后进行处理 有缓冲区 高性能
    // 2）将整体分区数据加载至内存后进行处理，处理完的数据不会释放 数量大较大、内存较容易出现内存溢出
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val mapRDD: RDD[Int] = dataRDD.mapPartitions(_.map(_ * 2))
    mapRDD.collect.foreach(println)
  }

  /**
   * mapPartitions标准操作[Value集合类型]
   * @param sc
   */
  def transformMapPartitions02(sc: SparkContext): Unit = {
    // TODO: 2.1 mapPartitions标准操作[Value集合类型]：读取单个分区的全部数据后进行逻辑处理
    // 1）以分区为单位进行数据转换操作 会将整体分区数据加载至内存后进行处理 有缓冲区 高性能
    // 2）将整体分区数据加载至内存后进行处理，处理完的数据不会释放 数量大较大、内存较容易出现内存溢出
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val mapRDD: RDD[Int] = dataRDD.mapPartitions(iter => {
      println("mapPartitions: >>>>>>>>>>>>>>>")
      iter.map(_ * 2)
    })

    mapRDD.collect()
  }

  /**
   * mapPartitions：获取每个分区的最大值
   * @param sc
   */
  def transformMapPartitions03(sc: SparkContext): Unit = {
    // TODO: 2.1 mapPartitions[Value集合类型]
    // 1）以分区为单位进行数据转换操作 会将整体分区数据加载至内存后进行处理 有缓冲区 高性能
    // 2）将整体分区数据加载至内存后进行处理，处理完的数据不会释放 数量大较大、内存较容易出现内存溢出
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val mapRDD: RDD[Int] = dataRDD.mapPartitions(iter => {
      List(iter.max).iterator
    })

    mapRDD.collect().foreach(println)
  }
}
