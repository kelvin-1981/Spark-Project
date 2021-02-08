package com.yykj.spark.core.mock

import org.apache.spark.{Dependency, Partition}

import scala.reflect.{ClassTag, classTag}

/**
 * 一、Spark RDD 特征
 * 1.具备弹性：存储、容错、计算、分区
 * 2.分布式：数据存储在分布式大数据平台 RDD进行分布式计算；
 * 3.计算：RDD是Spark最小计算单元，可以通过RDD组合完成丰富功能
 * 4.数据模型：用于封装计算逻辑，不保存数据
 * 5.抽象类
 * 6.不可变：RDD封装了计算逻辑，不可以改变。如需新计算逻辑需要编写新的RDD
 * 7.可分区：实现并行计算
 *
 * 二、Spark RDD 基类包含五大属性
 * 1.分区列表:getPartitions
 * 2.计算方法:compute
 * 3.RDD依赖关系:getDependencies
 * 4.分区器:paritioner
 * 5.首选位置:判断计算发送给哪个节点最有 优先发给本地有数据的计算节点
 */
abstract class SparkMockRDD[T: ClassTag](sc: SparkMockContext){

  /**
   * 1.[Spark一致]获取分区列表
   * @return
   */
  def getPartitions() : Array[SparkMockPartition]

  /**
   * 2.分布式计算方法
   * @param split
   * @return
   */
  def compute(split: SparkMockPartition) : Iterator[T]


  /**
   * 3.[Spark一致]依赖关系列表（本示例未使用）
   * @return
   */
  //protected def getDependencies: Seq[Dependency[_]] = deps
  protected def getDependencies: Seq[Int]

  /**
   * 4.[Spark一致]获取最佳计算位置
   * @param split
   * @return
   */
  protected def getPreferredLocations(split: Partition): Seq[String]

  /**
   * 5.[Spark一致]分区器
   */
  var paritioner : Option[SparkMockParitioner] = null

  /**
   * Return an array that contains all of the elements in this RDD.
   *
   * @note This method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   */
  def collect(): Array[T] = {
    //val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    //Array.concat(results: _*)
    null
  }

}
